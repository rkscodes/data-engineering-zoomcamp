from prefect.filesystems import GitHub
from prefect.deployments import Deployment
from etl_web_to_gc import etl_web_to_gcs

github_block = GitHub.load("homework-github")

github_dep = Deployment.build_from_flow(
    flow=etl_web_to_gcs, name="github-flow", storage=github_block, entrypoint="week_2/02_gcp/etl_web_to_gc.py"
)

if __name__ == "__main__":
    github_dep.apply()
