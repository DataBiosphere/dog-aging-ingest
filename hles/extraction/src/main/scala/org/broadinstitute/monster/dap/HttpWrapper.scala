
package org.broadinstitute.monster.dap

import okhttp3.Request
import upack.Msg

import scala.concurrent.Future

trait HttpWrapper {
  def makeRequest(request: Request):  Future[Msg]
}
