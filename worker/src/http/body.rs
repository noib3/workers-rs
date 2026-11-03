use std::{
    any::Any,
    cell::Cell,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};
use wasm_bindgen::JsCast;
use wasm_streams::readable::IntoStream;

use crate::Error;
use bytes::Bytes;
use futures_util::TryStream;
use futures_util::TryStreamExt;
use futures_util::{stream::FusedStream, Stream, StreamExt};
use http_body::{Body as HttpBody, Frame};
use js_sys::Uint8Array;
use pin_project::pin_project;
use wasm_bindgen::JsValue;

#[derive(Debug)]
pub struct Body(State);

unsafe impl Sync for Body {}
unsafe impl Send for Body {}

#[derive(Debug)]
enum State {
    Empty,
    /// The body's stream hasn't been read yet.
    Unread(UnreadStream),
    Reading(IntoStream<'static>),
}

/// The stream of a [`Body`] that hasn't been read yet.
///
/// [`passthrough`](crate::http::response::passthrough) shares it with
/// [`to_wasm`](crate::http::response::to_wasm) through the response's
/// extensions, so that `to_wasm` can hand it to the runtime as is even when it
/// can't see the body's type, as long as the body still hasn't read it or been
/// dropped.
#[derive(Clone)]
pub(crate) struct UnreadStream(Rc<Cell<Option<web_sys::ReadableStream>>>);

unsafe impl Sync for UnreadStream {}
unsafe impl Send for UnreadStream {}

impl Body {
    pub fn new(stream: web_sys::ReadableStream) -> Self {
        Self(State::Unread(UnreadStream(Rc::new(Cell::new(Some(
            stream,
        ))))))
    }

    pub fn into_inner(mut self) -> Option<web_sys::ReadableStream> {
        match std::mem::replace(&mut self.0, State::Empty) {
            State::Empty => None,
            State::Unread(unread) => unread.take(),
            State::Reading(stream) => {
                Some(wasm_streams::ReadableStream::from_stream(stream).into_raw())
            }
        }
    }

    pub fn empty() -> Self {
        Self(State::Empty)
    }

    /// Create a `Body` using a [`Stream`](futures_util::stream::Stream)
    pub fn from_stream<S>(stream: S) -> Result<Self, crate::Error>
    where
        S: TryStream + 'static,
        S::Ok: Into<Vec<u8>>,
        S::Error: std::fmt::Debug,
    {
        let js_stream = stream
            .map_ok(|item| -> Vec<u8> { item.into() })
            .map_ok(|chunk| {
                let array = Uint8Array::new_with_length(chunk.len() as _);
                array.copy_from(&chunk);
                array.into()
            })
            .map_err(|err| crate::Error::RustError(format!("{err:?}")))
            .map_err(|e| wasm_bindgen::JsValue::from(e.to_string()));

        let stream = wasm_streams::ReadableStream::from_stream(js_stream);
        let stream: web_sys::ReadableStream = stream.into_raw().dyn_into().unwrap();

        Ok(Self::new(stream))
    }

    /// Returns the body's stream if it hasn't been read yet.
    pub(crate) fn unread_stream(&self) -> Option<UnreadStream> {
        match &self.0 {
            State::Unread(unread) => Some(unread.clone()),
            State::Empty | State::Reading(_) => None,
        }
    }

    /// Returns the stream to read the body from, starting to read it if it
    /// hasn't been read yet.
    fn stream_mut(&mut self) -> Option<&mut IntoStream<'static>> {
        if let State::Unread(unread) = &self.0 {
            self.0 = match unread.take() {
                Some(stream) => State::Reading(into_stream(stream)),
                None => State::Empty,
            };
        }
        match &mut self.0 {
            State::Reading(stream) => Some(stream),
            State::Empty | State::Unread(_) => None,
        }
    }
}

impl UnreadStream {
    pub(crate) fn take(&self) -> Option<web_sys::ReadableStream> {
        self.0.take()
    }
}

impl std::fmt::Debug for UnreadStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("UnreadStream").finish_non_exhaustive()
    }
}

impl HttpBody for Body {
    type Data = Bytes;
    type Error = Error;

    #[inline]
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if let Some(stream) = self.stream_mut() {
            stream
                .poll_next_unpin(cx)
                .map_ok(|buf| {
                    let bytes = Bytes::copy_from_slice(&js_sys::Uint8Array::from(buf).to_vec());
                    Frame::data(bytes)
                })
                .map_err(Error::Internal)
        } else {
            Poll::Ready(None)
        }
    }

    #[inline]
    fn size_hint(&self) -> http_body::SizeHint {
        match &self.0 {
            State::Empty => http_body::SizeHint::with_exact(0),
            State::Unread(_) => http_body::SizeHint::new(),
            State::Reading(stream) => {
                let mut hint = http_body::SizeHint::new();
                let (lower, upper) = stream.size_hint();

                hint.set_lower(lower as u64);
                if let Some(upper) = upper {
                    hint.set_upper(upper as u64);
                }
                hint
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        match &self.0 {
            State::Empty => true,
            State::Unread(_) => false,
            State::Reading(stream) => stream.is_terminated(),
        }
    }
}

impl Drop for Body {
    fn drop(&mut self) {
        // A body dropped before being read was discarded or replaced, so
        // `to_wasm` must not send its stream.
        if let State::Unread(unread) = &self.0 {
            unread.take();
        }
    }
}

impl Stream for Body {
    type Item = Result<Bytes, Error>;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_frame(cx).map(|o| {
            if let Some(r) = o {
                match r {
                    Ok(f) => {
                        if f.is_data() {
                            let b = f.into_data().unwrap();
                            Some(Ok(b))
                        } else {
                            // Not sure how to handle trailers in Stream
                            None
                        }
                    }
                    Err(_) => Some(Err(Error::RustError("Error polling body".to_owned()))),
                }
            } else {
                None
            }
        })
    }
}

fn into_stream(stream: web_sys::ReadableStream) -> IntoStream<'static> {
    wasm_streams::ReadableStream::from_raw(stream.unchecked_into()).into_stream()
}

/// Converts the body of a request or response into a JS stream, or `None` if
/// it's empty.
///
/// A [`Body`] whose stream hasn't been read yet hands it back as is, whether
/// `body` is that [`Body`] or the [`UnreadStream`] in `extensions` belongs to
/// it. Any other body is streamed through Wasm.
pub(crate) fn into_readable_stream<B>(
    body: B,
    extensions: &mut http::Extensions,
) -> Option<web_sys::ReadableStream>
where
    B: http_body::Body<Data = Bytes> + 'static,
{
    let unread = (&body as &dyn Any)
        .downcast_ref::<Body>()
        .and_then(Body::unread_stream)
        .or_else(|| extensions.remove::<UnreadStream>());

    if let Some(stream) = unread.and_then(|unread| unread.take()) {
        return Some(stream);
    }

    // I'm not sure how we are supposed to determine if there is no
    // body for an `http_body::Body`, seems like this may be the only
    // option given the trait? This appears to work for things like
    // `hyper::Empty`.
    if body.is_end_stream() {
        None
    } else {
        let stream = BodyStream::new(body);
        Some(wasm_streams::ReadableStream::from_stream(stream).into_raw())
    }
}

#[pin_project]
pub(crate) struct BodyStream<B> {
    #[pin]
    inner: B,
}

impl<B> BodyStream<B> {
    pub(crate) fn new(inner: B) -> Self {
        Self { inner }
    }
}

impl<B: http_body::Body<Data = Bytes>> Stream for BodyStream<B> {
    type Item = std::result::Result<JsValue, JsValue>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let inner: Pin<&mut B> = this.inner;
        inner.poll_frame(cx).map(|o| {
            if let Some(r) = o {
                match r {
                    Ok(f) => {
                        if f.is_data() {
                            // Should not be Err after checking on previous line
                            let b = f.into_data().unwrap();
                            let array = Uint8Array::new_with_length(b.len() as _);
                            array.copy_from(&b);
                            Some(Ok(array.into()))
                        } else {
                            None
                        }
                    }
                    Err(_) => Some(Err(JsValue::from_str("Error polling body"))),
                }
            } else {
                None
            }
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let hint = self.inner.size_hint();
        (hint.lower() as usize, hint.upper().map(|u| u as usize))
    }
}
