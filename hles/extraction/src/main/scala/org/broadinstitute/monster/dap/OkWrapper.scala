package org.broadinstitute.monster.dap

import java.io.IOException
import java.time.Duration

import okhttp3.{Call, Callback, OkHttpClient, Request, Response}
import org.broadinstitute.monster.common.msg.JsonParser
import upack.Msg

import scala.concurrent.{Future, Promise}

class OkWrapper extends HttpWrapper {
  /** Timeout to use for all requests to production RedCap. */
  private val timeout = Duration.ofSeconds(60)

  /** Construct a client instance backed by the production RedCap instance. */
  def client =
    new OkHttpClient.Builder()
      .connectTimeout(timeout)
      .readTimeout(timeout)
      .build()

  def makeRequest(request: Request): Future[Msg] = {
    val p = Promise[Msg]()
    client
      .newCall(request)
      .enqueue(new Callback {
        override def onFailure(call: Call, e: IOException): Unit =
          p.failure(e)
        override def onResponse(call: Call, response: Response): Unit = {
          val maybeResult =
            JsonParser.parseEncodedJsonReturningFailure(response.body().string())
          maybeResult match {
            case Right(result) => p.success(result)
            case Left(err)     => p.failure(err)
          }
        }
      })
    p.future
  }
}
