package org.broadinstitute.monster.dap

import okhttp3.Request
import upack.Msg

import scala.concurrent.Future

trait HTTPWrapper {
  def makeRequest(request: Request):  Future[Msg]
}


