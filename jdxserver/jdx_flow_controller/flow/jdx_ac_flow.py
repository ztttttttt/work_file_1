from mlx_flow.mbd_flow import MBDFlowType
from mlx_utility import config_manager as cm


def get():
    return {
        "flow_name": "JDX_AC",  # should same as the status in the match of the first receive msg
        "flows": [
            {
                "name": "receive_jdx_ac_ml_job",
                "type": MBDFlowType.RECEIVE_MESSAGE,
                "match": {"data": {"name": "MachineLearning", "status": "JDX_AC"}},
                "vars": [
                    {"var_key": "app_id", "msg_key": "data.appId"},
                ]
            },
            {
                "name": "send_jdx_ac_to_worker_job",
                "type": MBDFlowType.SEND_MESSAGE,
                "queue": cm.config['queue']['worker'],
                "message": {
                    "app_id": ("var", "app_id"),
                    "job_name": ("val", "run_worker")
                }
            },
            {
                "name": "receive_jdx_ac_woker_result_job",
                "type": MBDFlowType.RECEIVE_MESSAGE,
                "match": {"job_name": "run_worker"},
                "vars": [
                    {"var_key": "app_id", "msg_key": "app_id"},
                    {"var_key": "user_id", "msg_key": "user_id"}
                ]
            },
            {
                "name": "send_jdx_ac_graph_job",
                "type": MBDFlowType.SEND_MESSAGE,
                "queue": cm.config['queue']['graph'],
                "message": {
                    "app_id": ("var", "app_id"),
                    "product_name": ("val", "jdx"),
                    "job_name": ("val", "jdx_graph_features")
                }
            },
            {
                "name": "receive_jdx_ac_graph_job",
                "type": MBDFlowType.RECEIVE_MESSAGE,
                "match": {"job_name": "jdx_graph_features"}
            },
            {
                "name": "send_jdx_ac_graph_feature_job_timeout_alarm",
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
                "name": "send_jdx_ac_determine_category_job",
                "type": MBDFlowType.SEND_MESSAGE,
                "queue": cm.config['queue']['category'],
                "message": {
                    "app_id": ("var", "app_id"),
                    "user_id": ("var", "user_id"),
                    "job_name": ("val", "run_category")
                }
            },
            {
                "name": "receive_jdx_ac_determine_category_job",
                "type": MBDFlowType.RECEIVE_MESSAGE,
                "match": {"job_name": "run_category"},
                "vars": [
                    {"var_key": "category_id", "msg_key": "category_id"}
                ]
            },
            {
                "name": "send_jdx_ac_rule_check_job",
                "type": MBDFlowType.SEND_MESSAGE,
                "queue": cm.config['queue']['ruler'],
                "message": {
                    "app_id": ("var", "app_id"),
                    "user_id": ("var", "user_id"),
                    "job_name": ("val", "run_rule"),
                    "flow_type": ("val", "c"),
                    "category_id": ("var", "category_id")
                }
            },
            {
                "name": "receive_jdx_ac_rule_check_result_job",
                "type": MBDFlowType.RECEIVE_MESSAGE,
                "match": {"job_name": "run_rule"},
                "vars": [
                    {"var_key": "group_tag", "msg_key": "group_tag"},
                    {"var_key": "hit_rule_str", "msg_key": "hit_rule_str"},
                    {"var_key": "rule_pass", "msg_key": "rule_pass"}
                ]
            },
            {
                "name": "send_jdx_ac_run_model_job",
                "type": MBDFlowType.SEND_MESSAGE,
                "queue": cm.config['queue']['model'],
                "message": {
                    "app_id": ("var", "app_id"),
                    "user_id": ("var", "user_id"),
                    "job_name": ("val", "run_model"),
                    "category_id": ("var", "category_id")
                }
            },
            {
                "name": "receive_jdx_ac_model_result_job",
                "type": MBDFlowType.RECEIVE_MESSAGE,
                "match": {"job_name": "run_model"},
                "vars": [
                    {"var_key": "model_name", "msg_key": "model_name"},
                    {"var_key": "model_score", "msg_key": "model_score"},
                    {"var_key": "op_score", "msg_key": "op_score"}
                ]
            },
            {
                "name": "send_jdx_ac_run_credit_job",
                "type": MBDFlowType.SEND_MESSAGE,
                "queue": cm.config['queue']['credit'],
                "message": {
                    "app_id": ("var", "app_id"),
                    "job_name": ("val", "run_credit"),
                    "category_id": ("var", "category_id"),
                    "model_name": ("var", "model_name"),
                    "model_score": ("var", "model_score"),
                    "group_tag": ("var", "group_tag"),
                    "op_score": ("var", "op_score")
                }
            },
            {
                "name": "receive_jdx_ac_credit_result_job",
                "type": MBDFlowType.RECEIVE_MESSAGE,
                "match": {"job_name": "run_credit"},
                "vars": [
                    {"var_key": "credit_amount", "msg_key": "credit_amount"},
                    {"var_key": "model_pass", "msg_key": "model_pass"}
                ]
            },
            {
                "name": "send_jdx_ac_judger_job",
                "type": MBDFlowType.SEND_MESSAGE,
                "queue": cm.config['queue']['judger'],
                "message": {
                    "job_name": ("val", "final_ac_judger"),
                    "app_id": ("var", "app_id"),
                    "user_id": ("var", "user_id"),
                    "category_id": ("var", "category_id"),
                    "rule_result": ("var", "hit_rule_str"),
                    "model_pass": ("var", "model_pass"),
                    "server": ("val", "open_card")
                }
            },
            {
                "name": "receive_jdx_ac_judger_result_job",
                "type": MBDFlowType.RECEIVE_MESSAGE,
                "match": {"job_name": "final_ac_judger"},
                "vars": [
                    {"var_key": "final_result", "msg_key": "final_result"},
                    {"var_key": "result", "msg_key": "result"},
                    # {"var_key": "credit_amount", "msg_key": "credit_amount"},
                    {"var_key": "rejectReason", "msg_key": "rejectReason"}
                ]
            },
            {
                "name": "send_jdx_ac_final_result_job",
                "type": MBDFlowType.SEND_MESSAGE,
                "queue": cm.config['queue']['final_result'],
                "message": {
                    "product" : ("val","jdcl"),
                    "fromQueue" : ("val","JdclJobStatusQueue"),
                    "backQueue" : ("val","JdclJobStatusQueue"),
                    "data" : {
                        "appId" : ("var", "app_id"),
                        "name" : ("val","MachineLearning"),
                        "status" : ("var", "result"),
                        "extraInfo" : {
                            "credit_results" : [
                                {
                                    "target" : ("val", "jd"),
                                    "is_pass" : ("var", "result"),
                                    "credit_amount" : ("var", "credit_amount"),
                                    "reject_reason" : ("var", "rejectReason")
                                },
                                {
                                    "target" : ("val", "ctl"),
                                    "is_pass" : ("val", "Rejected"),
                                    "credit_amount" : ("val", "0"),
                                    "reject_reason" : ("val", "")
                                }
                            ],
                            "groupTag" : ("var", "group_tag")
                        }
                    }
                }
            },
        ],
        "dependencies": [
            {
                "upstream": ["receive_jdx_ac_ml_job"],
                "downstream": ["send_jdx_ac_to_worker_job"]
            },
            {
                "upstream": ["receive_jdx_ac_woker_result_job"],
            #     "downstream": ["send_jdx_ac_graph_job", "send_jdx_ac_graph_feature_job_timeout_alarm"]
            # },
            # {
            #     "upstream": ["receive_jdx_ac_graph_job"],
                "downstream": ["send_jdx_ac_determine_category_job"]
            },
            {
                "upstream": ["receive_jdx_ac_determine_category_job"],
                "downstream": ["send_jdx_ac_rule_check_job"]
            },
            {
                "upstream": ["receive_jdx_ac_rule_check_result_job"],
                "downstream": ["send_jdx_ac_run_model_job"]
            },
            {
                "upstream": ["receive_jdx_ac_model_result_job"],
                "downstream": ["send_jdx_ac_run_credit_job"]
            },
            {
                "upstream": ["receive_jdx_ac_credit_result_job"],
                "downstream": ["send_jdx_ac_judger_job"]
            },
            {
                "upstream": ["receive_jdx_ac_judger_result_job"],
                "downstream": ["send_jdx_ac_final_result_job"]
            },
        ]
    }
