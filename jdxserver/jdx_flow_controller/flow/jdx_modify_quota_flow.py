from mlx_flow.mbd_flow import MBDFlowType
from mlx_utility import config_manager as cm


def get():
    return {
        "flow_name": "JDX_QUOTA",  # should same as the status in the match of the first receive msg
        "flows": [
            {
                "name": "receive_jdx_quota_ml_job",
                "type": MBDFlowType.RECEIVE_MESSAGE,
                "match": {"data": {"name": "MachineLearningForCredit", "status": "JDX_QUOTA"}},
                "vars": [
                    {"var_key": "app_id", "msg_key": "data.appId"},
                ]
            },
            {
                "name": "send_jdx_quota_to_worker_job",
                "type": MBDFlowType.SEND_MESSAGE,
                "queue": cm.config['queue']['worker'],
                "message": {
                    "app_id": ("var", "app_id"),
                    "job_name": ("val", "run_worker")
                }
            },
            {
                "name": "receive_jdx_quota_woker_result",
                "type": MBDFlowType.RECEIVE_MESSAGE,
                "match": {"job_name": "run_worker"},
                "vars": [
                    {"var_key": "app_id", "msg_key": "app_id"},
                    {"var_key": "user_id", "msg_key": "user_id"},
                    {"var_key": "job_name", "msg_key": "job_name"}
                ]
            },
            {
                "name": "send_jdx_quota_inner_worker_job",
                "type": MBDFlowType.SEND_MESSAGE,
                "queue": cm.config['queue']['worker'],
                "message": {
                    "app_id": ("var", "app_id"),
                    "job_name": ("val", "collect_inner_data"),
                    "reply_required": ("val", 1),
                    "source_type": ("val", "q")
                }
            },
            {
                "name": "receive_jdx_quota_inner_woker_done",
                "type": MBDFlowType.RECEIVE_MESSAGE,
                "match": {"job_name": "collect_inner_data", "source_type": "q"},
                "vars": [
                    {"var_key": "app_id", "msg_key": "app_id"},
                    {"var_key": "job_name", "msg_key": "job_name"}
                ]
            },
            {
                "name": "send_jdx_quota_mx_worker_job",
                "type": MBDFlowType.SEND_MESSAGE,
                "queue": cm.config['queue']['worker'],
                "message": {
                    "app_id": ("var", "app_id"),
                    "job_name": ("val", "collect_mx_data"),
                    "reply_required": ("val", 1),
                    "source_type": ("val", "q")
                }
            },
            {
                "name": "receive_jdx_quota_mx_woker_done",
                "type": MBDFlowType.RECEIVE_MESSAGE,
                "match": {"job_name": "collect_mx_data", "source_type": "q"},
                "vars": [
                    {"var_key": "app_id", "msg_key": "app_id"},
                    {"var_key": "job_name", "msg_key": "job_name"}
                ]
            },
            {
                "name": "send_jdx_quota_mxfund_worker_job",
                "type": MBDFlowType.SEND_MESSAGE,
                "queue": cm.config['queue']['worker'],
                "message": {
                    "app_id": ("var", "app_id"),
                    "job_name": ("val", "collect_mxfund_data"),
                    "reply_required": ("val", 1),
                    "source_type": ("val", "q")
                }
            },
            {
                "name": "receive_jdx_quota_mxfund_woker_done",
                "type": MBDFlowType.RECEIVE_MESSAGE,
                "match": {"job_name": "collect_mxfund_data", "source_type": "q"},
                "vars": [
                    {"var_key": "app_id", "msg_key": "app_id"},
                    {"var_key": "job_name", "msg_key": "job_name"}
                ]
            },
            {
                "name": "send_jdx_quota_graph_job",
                "type": MBDFlowType.SEND_MESSAGE,
                "queue": cm.config['queue']['graph'],
                "message": {
                    "app_id": ("var", "app_id"),
                    "product_name": ("val", "jdx"),
                    "job_name": ("val", "jdx_graph_features")
                }
            },
            {
                "name": "receive_jdx_quota_graph_job",
                "type": MBDFlowType.RECEIVE_MESSAGE,
                "match": {"job_name": "jdx_graph_features"}
            },
            {
                "name": "send_jdx_quota_graph_feature_job_timeout_alarm",
                "type": MBDFlowType.SEND_MESSAGE,
                "queue": cm.config['queue_server']['name'],
                "message": {
                    "app_id": ("var", "app_id"),
                    "job_name": ("val", "jdx_graph_features"),
                    "is_timeout": ("val", 1)
                },
                "delayed_seconds": 10
            },
            {
                "name": "send_jdx_quota_determine_category_job",
                "type": MBDFlowType.SEND_MESSAGE,
                "queue": cm.config['queue']['category'],
                "message": {
                    "app_id": ("var", "app_id"),
                    "user_id": ("var", "user_id"),
                    "job_name": ("val", "determine_category_quota")
                }
            },
            {
                "name": "receive_jdx_quota_determine_category_job",
                "type": MBDFlowType.RECEIVE_MESSAGE,
                "match": {"job_name": "determine_category_quota"},
                "vars": [
                    {"var_key": "category_id", "msg_key": "category_id"}
                ]

            },
            {
                "name": "send_jdx_quota_run_model_job",
                "type": MBDFlowType.SEND_MESSAGE,
                "queue": cm.config['queue']['quota_model'],
                "message": {
                    "app_id": ("var", "app_id"),
                    "job_name": ("val", "run_quota_model"),
                    "category_id": ("var", "category_id")
                }
            },
            {
                "name": "receive_jdx_quota_model_result_job",
                "type": MBDFlowType.RECEIVE_MESSAGE,
                "match": {"job_name": "run_quota_model"},
                "vars": [
                    {"var_key": "model_name", "msg_key": "model_name"},
                    {"var_key": "model_score", "msg_key": "model_score"}
                ]
            },
            {
                "name": "send_jdx_quota_run_modify_job",
                "type": MBDFlowType.SEND_MESSAGE,
                "queue": cm.config['queue']['modify_quota'],
                "message": {
                    "app_id": ("var", "app_id"),
                    "job_name": ("val", "run_modify_quota"),
                    "category_id": ("var", "category_id"),
                    "model_name": ("var", "model_name"),
                    "model_score": ("var", "model_score")
                }
            },
            {
                "name": "receive_jdx_quota_modify_result_job",
                "type": MBDFlowType.RECEIVE_MESSAGE,
                "match": {"job_name": "run_modify_quota"},
                "vars": [
                    {"var_key": "final_quota", "msg_key": "final_quota"},
                ]
            },
            {
                "name": "send_jdx_quota_final_result_job",
                "type": MBDFlowType.SEND_MESSAGE,
                "queue": cm.config['queue']['final_result'],
                "message": {
                    "product": ("val", "jdcl"),
                    "fromQueue": ("val", "JdclJobStatusQueue"),
                    "backQueue": ("val", "JdclJobStatusQueue"),
                    "data": {
                        "appId": ("var", "app_id"),
                        "name": ("val", "MachineLearningForCredit"),
                        "status": ("var", ""),
                        "extraInfo": {
                            "quota": ("var", "final_quota")
                        }
                    }
                }
            },
        ],
        "dependencies": [
            {
                "upstream": ["receive_jdx_quota_ml_job"],
                "downstream": ["send_jdx_quota_to_worker_job"]
            },
            {
                "upstream": ["receive_jdx_quota_woker_result"],
                "downstream": ["send_jdx_quota_determine_category_job"]
            },
            {
                "upstream": ["receive_jdx_quota_determine_category_job"],
                "downstream": ["send_jdx_quota_run_model_job"]
            },
            {
                "upstream": ["receive_jdx_quota_model_result_job"],
                "downstream": ["send_jdx_quota_run_modify_job"]
            },
            {
                "upstream": ["receive_jdx_quota_modify_result_job"],
                "downstream": ["send_jdx_quota_final_result_job"]
            },
        ]
    }
