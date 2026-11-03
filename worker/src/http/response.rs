use super::header::{header_map_from_web_sys_headers, web_sys_headers_from_header_map};
use crate::http::body::{into_readable_stream, Body};
use crate::HttpResponse;
use crate::Result;
use crate::WebSocket;
use bytes::Bytes;

use crate::response::EncodeBody;
use crate::CfResponseProperties;
use crate::Headers;
use crate::ResponseBuilder;
use worker_sys::ext::ResponseExt;

/// **Requires** `http` feature. Convert generic [`http::Response<B>`](crate::HttpResponse)
/// to [`web_sys::Response`](web_sys::Response) where `B` can be any [`http_body::Body`](http_body::Body)
///
/// If the body is a [`Body`] that hasn't been read yet, or the response went
/// through [`passthrough`], its stream is handed to the runtime as is instead
/// of being streamed through Wasm.
pub fn to_wasm<B>(mut res: http::Response<B>) -> Result<web_sys::Response>
where
    B: http_body::Body<Data = Bytes> + 'static,
{
    let headers = web_sys_headers_from_header_map(res.headers())?;
    let mut init = ResponseBuilder::new()
        .with_status(res.status().as_u16())
        .with_headers(Headers(headers));

    if let Some(ws) = res.extensions_mut().remove::<WebSocket>() {
        init = init.with_websocket(ws);
    }
    if let Some(encode_body) = res.extensions_mut().remove::<EncodeBody>() {
        init = init.with_encode_body(encode_body);
    }
    if let Some(CfResponseProperties(obj)) = res.extensions_mut().remove::<CfResponseProperties>() {
        init = init.with_cf_raw(obj);
    }

    let (mut parts, body) = res.into_parts();
    let readable_stream = into_readable_stream(body, &mut parts.extensions);

    Ok(web_sys::Response::new_with_opt_readable_stream_and_init(
        readable_stream.as_ref(),
        &init.into(),
    )?)
}

/// **Requires** `http` feature. Convert [`web_sys::Response`](web_sys::Response)
/// to [`worker::HttpResponse`](crate::HttpResponse)
pub fn from_wasm(res: web_sys::Response) -> Result<HttpResponse> {
    let mut builder =
        http::response::Builder::new().status(http::StatusCode::from_u16(res.status())?);
    if let Some(headers) = builder.headers_mut() {
        header_map_from_web_sys_headers(res.headers(), headers)?;
    }
    if let Some(ws) = res.websocket() {
        builder = builder.extension(WebSocket::from(ws));
    }
    if let Some(cf) = res.cf() {
        builder = builder.extension(CfResponseProperties(cf));
    }
    Ok(if let Some(body) = res.body() {
        builder.body(Body::new(body))?
    } else {
        builder.body(Body::empty())?
    })
}

/// **Requires** `http` feature. Make [`to_wasm`] hand the response's body to
/// the runtime as is, instead of streaming it through Wasm, even after the
/// body's type has been erased, e.g. by converting the response into an
/// `axum` one.
///
/// This avoids copying every chunk of the body into Wasm memory and back, e.g.
/// when returning the response of a subrequest, or an object read from R2, KV
/// or the Cache API. [`to_wasm`] already does that if it gets the [`Body`]
/// itself.
///
/// If the body is read, or dropped (for example because a middleware replaced
/// it) before reaching [`to_wasm`], it's streamed through Wasm as usual. A
/// middleware that wraps the body without reading it, e.g. to compress it, is
/// bypassed, so only use this for bodies that must reach the client untouched.
pub fn passthrough(mut res: HttpResponse) -> HttpResponse {
    if let Some(unread) = res.body().unread_stream() {
        res.extensions_mut().insert(unread);
    }
    res
}

#[cfg(feature = "axum")]
impl From<crate::Response> for http::Response<axum::body::Body> {
    fn from(resp: crate::Response) -> http::Response<axum::body::Body> {
        let res: web_sys::Response = resp.into();
        let mut builder = http::response::Builder::new()
            .status(http::StatusCode::from_u16(res.status()).unwrap());
        if let Some(headers) = builder.headers_mut() {
            crate::http::header::header_map_from_web_sys_headers(res.headers(), headers).unwrap();
        }
        if let Some(ws) = res.websocket() {
            builder = builder.extension(WebSocket::from(ws));
        }
        if let Some(cf) = res.cf() {
            builder = builder.extension(CfResponseProperties(cf));
        }
        if let Some(body) = res.body() {
            builder
                .body(axum::body::Body::new(crate::Body::new(body)))
                .unwrap()
        } else {
            builder
                .body(axum::body::Body::new(crate::Body::empty()))
                .unwrap()
        }
    }
}
