from mlx_server.daemon import Daemon
from mlx_server.mbd_flow_server import MBDFlowServer
from flow import jdx_ac_flow, jdx_withdraw_flow, jdx_modify_quota_flow


class JDXFlowController(Daemon):
    def setup_server(self):
        server = MBDFlowServer(mbd_flows=[jdx_ac_flow.get(), jdx_withdraw_flow.get(), jdx_modify_quota_flow.get()])
        self.server = server


if __name__ == '__main__':
    JDXFlowController().main()
