package es.lifk.braveInstrumentationKtor

import brave.Tracing
import brave.http.HttpClientAdapter
import brave.http.HttpTracing
import io.ktor.client.*
import io.ktor.client.features.*
import io.ktor.client.request.*
import io.ktor.util.*
import brave.http.HttpClientHandler
import brave.propagation.Propagation
import io.ktor.client.response.HttpReceivePipeline
import io.ktor.client.response.HttpResponse

class BraveTracing(httpTracing: HttpTracing) {
    private val setter = Propagation.Setter<HttpRequestBuilder, String> { carrier, key, value -> carrier.header(key, value)}
    val tracer = httpTracing.tracing().tracer()!!
    val handler = HttpClientHandler.create(httpTracing, HttpAdapter())!!
    val injector = httpTracing.tracing().propagation().injector(setter)!!

    class Configuration {
        lateinit var tracing: Tracing
        internal fun build(): BraveTracing = BraveTracing(HttpTracing.create(tracing))
    }

    companion object Feature : HttpClientFeature<Configuration, BraveTracing> {
        override val key: AttributeKey<BraveTracing> = AttributeKey("BraveTracingHeader")
        override fun prepare(block: Configuration.() -> Unit): BraveTracing = Configuration().apply(block).build()
        override fun install(feature: BraveTracing, scope: HttpClient) {
            scope.sendPipeline.intercept(HttpSendPipeline.Before) {
                val span = feature.handler.handleSend(feature.injector, context)
                if (!span.isNoop) {
                    span.remoteIpAndPort(context.host, context.port)
                    feature.tracer.withSpanInScope(span)
                }
            }

            scope.receivePipeline.intercept(HttpReceivePipeline.After) {
                feature.handler.handleReceive(context.response, null, feature.tracer.currentSpan())
            }
        }
    }

    class HttpAdapter : HttpClientAdapter<HttpRequestBuilder, HttpResponse>() {
        override fun method(request: HttpRequestBuilder): String = request.method.value
        override fun path(request: HttpRequestBuilder): String = request.url.encodedPath
        override fun url(request: HttpRequestBuilder): String = request.url.encodedPath
        override fun requestHeader(request: HttpRequestBuilder, name: String): String = request.headers[name] ?: ""
        override fun statusCode(response: HttpResponse): Int = statusCodeAsInt(response)
        override fun statusCodeAsInt(response: HttpResponse): Int = response.status.value
    }
}