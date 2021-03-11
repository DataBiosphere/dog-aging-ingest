package org.broadinstitute.monster.dap.common

import okhttp3.Request
import upack.Msg

import scala.concurrent.Future

trait HttpWrapper extends Serializable {
  def makeRequest(request: Request): Future[Msg]
}
