package link.rdcn.dacp.catalog

import link.rdcn.server.module.{ActionMethod, CollectActionMethodEvent, CollectGetStreamMethodEvent}
import link.rdcn.server.{Anchor, CrossModuleEvent, DftpActionRequest, DftpActionResponse, DftpModule, EventHandler, ServerContext}

/**
 * @Author renhao
 * @Description:
 * @Data 2026/1/28 14:00
 * @Modified By:
 */
class DatasetMetaDataModule(datasetInfoProvider: DatasetInfoProvider) extends DftpModule {

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    anchor.hook(new EventHandler {
      override def accepts(event: CrossModuleEvent): Boolean =
        event match {
          case _: CollectActionMethodEvent => true
          case _ => false
        }

      override def doHandleEvent(event: CrossModuleEvent): Unit = {
        event match {
          case r: CollectActionMethodEvent => r.collect(new ActionMethod{

            override def accepts(request: DftpActionRequest): Boolean = {
              request.getActionName() == "GET_DATASET_INFO"
            }

            override def doAction(request: DftpActionRequest, response: DftpActionResponse): Unit = {
              require(request.getActionName() == "GET_DATASET_INFO")
              val params = request.getRequestParameters()
              val datasetId = params.getString("datasetId")
              response.sendJSONObject(datasetInfoProvider.getDatasetInfo(datasetId).toJson())
            }
          })
          case _ =>
        }
      }
    })
  }

  override def destroy(): Unit = {}
}

trait DatasetInfoProvider{
  def getDatasetInfo(datasetId: String): DatasetMetaData
}


