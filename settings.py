# general settings

DEFAULT_OUTPUT_DIR = 'logs'
DEFAULT_PROJECT = 'project-0'

# submitter settings

SUBMIT_USING_KUBECTL = True  # toggle submit using kubectl apply, or runai cli
RUNAI_CLI_PATH = 'runai'  # './runaiCli'

# sampler settings

TEST_SCHEDULER_EVENT_TIMES = False # required for eviction & pvc binding times

TEST_BACKEND_TIMES = False # required for ui (control-plane) times --- NOT AVAILABLE OUTSIDE RUN:AI NETWORK

IS_SELF_HOSTED_DB = False

SELF_HOSTED_DB_NAME = ''
SELF_HOSTED_DB_USER = ''
SELF_HOSTED_DB_PASSWORD = ''

STAGING_DB_HOST = ''
STAGING_DB_REGION = ''
STAGING_DB_NAME = ''
STAGING_DB_USER = ''
STAGING_RUNAI_CLUSTER_UUID = ''

# plotter settings

SAVE_PNG = True
