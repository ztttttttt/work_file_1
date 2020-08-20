from mlx_flow.mbd_flow import MBDFlowType
from mlx_utility import config_manager as cm


def get():
    return {
        "flow_name": "JDX_WITHDRAW",
        "flows": [
            {
                "name": "receive_jdx_withdraw_ml_job",
                "type": MBDFlowType.RECEIVE_MESSAGE,
                "match": {"data": {"name": "MachineLearningForBuy", "status": "JDX_WITHDRAW"}},
                "vars": [
                    {"var_key": "app_id", "msg_key": "data.appId"},
                ]
            },
            {
                "name": "send_jdx_withdraw_to_worker_job",
                "type": MBDFlowType.SEND_MESSAGE,
                "queue": cm.config['queue']['worker'],
                "message": {
                    "app_id": ("var", "app_id"),
                    "job_name": ("val", "run_worker")
                }
            },
            {
                "name": "receive_jdx_withdraw_woker_result",
                "type": MBDFlowType.RECEIVE_MESSAGE,
                "match": {"job_name": "run_worker"},
                "vars": [
                    {"var_key": "app_id", "msg_key": "app_id"},
                    {"var_key": "user_id", "msg_key": "user_id"}
                ]
            },
            {
                "name": "send_jdx_withdraw_determine_category_job",
                "type": MBDFlowType.SEND_MESSAGE,
                "queue": cm.config['queue']['category'],
                "message": {
                    "app_id": ("var", "app_id"),
                    "user_id": ("var", "user_id"),
                    "job_name": ("val", "determine_category_withdraw")
                }
            },
            {
                "name": "receive_jdx_withdraw_determine_category_job",
                "type": MBDFlowType.RECEIVE_MESSAGE,
                "match": {"job_name": "determine_category_withdraw"},
                "vars": [
                    {"var_key": "category_id", "msg_key": "category_id"}
                ]
            },
            {
                "name": "send_jdx_withdraw_rule_check_job",
                "type": MBDFlowType.SEND_MESSAGE,
                "queue": cm.config['queue']['ruler'],
                "message": {
                    "app_id": ("var", "app_id"),
                    "user_id": ("var", "user_id"),
                    "job_name": ("val", "run_rule"),
                    "flow_type": ("val", "w"),
                    "category_id": ("var", "category_id")
                }
            },
            {
                "name": "receive_jdx_withdraw_rule_check_result_job",
                "type": MBDFlowType.RECEIVE_MESSAGE,
                "match": {"job_name": "run_rule"},
                "vars": [
                    {"var_key": "group_tag", "msg_key": "group_tag"},
                    {"var_key": "hit_rule_str", "msg_key": "hit_rule_str"},
                    {"var_key": "rule_pass", "msg_key": "rule_pass"}
                ]
            },
            {
                "name": "send_jdx_withdraw_model_job",
                "type": MBDFlowType.SEND_MESSAGE,
                "queue": cm.config['queue']['withdraw_model'],
                "message": {
                    "app_id": ("var", "app_id"),
                    "user_id": ("var", "user_id"),
                    "group_tag": ("var", "group_tag"),
                    "job_name": ("val", "run_withdraw_model"),
                    "category_id": ("var", "category_id")
                }
            },
            {
                "name": "receive_jdx_withdraw_model_job",
                "type": MBDFlowType.RECEIVE_MESSAGE,
                "match": {"job_name": "run_withdraw_model"},
                "vars": [
                    {"var_key": "model_pass", "msg_key": "model_pass"},
                    {"var_key": "op_score", "msg_key": "op_score"}
                ]
            },
            {
                "name": "send_jdx_withdraw_judger_job",
                "type": MBDFlowType.SEND_MESSAGE,
                "queue": cm.config['queue']['judger'],
                "message": {
                    "job_name": ("val", "final_withdraw_judger"),
                    "app_id": ("var", "app_id"),
                    "user_id": ("var", "user_id"),
                    "category_id": ("var", "category_id"),
                    "rule_result": ("var", "hit_rule_str"),
                    "model_pass":("var", "model_pass"),
                    "server":("val", "withdraw")
                }
            },
            {
                "name": "receive_jdx_withdraw_judger_result_job",
                "type": MBDFlowType.RECEIVE_MESSAGE,
                "match": {"job_name": "final_withdraw_judger"},
                "vars": [
                    {"var_key": "final_result", "msg_key": "final_result"},
                    {"var_key": "result", "msg_key": "result"},
                    {"var_key": "rejectReason", "msg_key": "rejectReason"}
                ]
            },
            {
                "name": "send_jdx_withdraw_final_result_job",
                "type": MBDFlowType.SEND_MESSAGE,
                "queue": cm.config['queue']['final_result'],
                "message": {
                    "product": ("val", "jdcl"),
                    "fromQueue": ("val", "JdclJobStatusQueue"),
                    "backQueue": ("val", "JdclJobStatusQueue"),
                    "data": {
                        "appId": ("var", "app_id"),
                        "name": ("val", "MachineLearningForBuy"),
                        "status": ("var", "result"),
                        "extraInfo":  {
                            "rejectReason" : ("var", "rejectReason"),
                            "groupTag" : ("var", "group_tag"),
                            "target" : ("val", "jd")
                        }
                    }
                }
            }
        ],
        "dependencies": [
            {
                "upstream": ["receive_jdx_withdraw_ml_job"],
                "downstream": ["send_jdx_withdraw_to_worker_job"]
            },
            {
                "upstream": ["receive_jdx_withdraw_woker_result"],
                "downstream": ["send_jdx_withdraw_determine_category_job"]
            },
            {
                "upstream": ["receive_jdx_withdraw_determine_category_job"],
                "downstream": ["send_jdx_withdraw_rule_check_job"]
            },
            {
                "upstream": ["receive_jdx_withdraw_rule_check_result_job"],
                "downstream": ["send_jdx_withdraw_model_job"]
            },
            {
                "upstream": ["receive_jdx_withdraw_model_job"],
                "downstream": ["send_jdx_withdraw_judger_job"]
            },
            {
                "upstream": ["receive_jdx_withdraw_judger_result_job"],
                "downstream": ["send_jdx_withdraw_final_result_job"]
            }
        ]
    }
