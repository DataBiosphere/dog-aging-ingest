package org.broadinstitute.monster.dap.common

import okhttp3._
import okhttp3.logging.HttpLoggingInterceptor
import okhttp3.logging.HttpLoggingInterceptor.{Level, Logger}
import org.broadinstitute.monster.common.msg.JsonParser
import org.slf4j.LoggerFactory
import upack.Msg

import java.io.IOException
import java.time.Duration
import scala.concurrent.{Future, Promise}

class OkWrapper extends HttpWrapper {
  /** Timeout to use for all requests to production RedCap. */
  private val timeout = Duration.ofSeconds(300)
  private val logger = LoggerFactory.getLogger(getClass)

  /** Construct a client instance backed by the production RedCap instance. */
  def client =
    new OkHttpClient.Builder()
      .connectTimeout(timeout)
      .readTimeout(timeout)
      .addInterceptor(new HttpLoggingInterceptor(new Logger() {

        override def log(s: String): Unit = {
          logger.info(s)
        }
      }).setLevel(Level.HEADERS))
      .addInterceptor((chain: Interceptor.Chain) => {
        val request = chain.request()
        var response = chain.proceed(request)
        var tryCount = 0
        while (!response.isSuccessful && tryCount < 3) {
          tryCount += 1
          response = chain.proceed(request)
        }
        response
      })
      .build()

  def makeRequest(request: Request): Future[Msg] = {
    val p = Promise[Msg]()
    client
      .newCall(request)
      .enqueue(new Callback {
        override def onFailure(call: Call, e: IOException): Unit =
          p.failure(e)
        override def onResponse(call: Call, response: Response): Unit = {
          if (!response.isSuccessful) {
            throw new Exception(
              s"Non-successful HTTP error code received [response.code=${response.code()}]"
            )
          }

          val responseBodyString = response.body().string()
          val maybeResult =
            JsonParser.parseEncodedJsonReturningFailure(responseBodyString)
          maybeResult match {
            case Right(result) => p.success(result)
            case Left(err)     => p.failure(err)
          }
        }
      })
    p.future
  }
}
