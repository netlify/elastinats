node {
  def err = null
  def project = "elastinats"

  stage "Checkout code"
    checkout scm

  stage "Run CI script"
    try {
      sh "script/ci.sh ${project}"
    } catch (Exception e) {
      currentBuild.result = "FAILURE"
      err = e
    }

  stage "Deploy"
    if (!err) {
      sh "release.sh ${project}"
    }

  stage "Notify"
    def message = "succeeded"
    def color = "good"

    if (currentBuild.result == "FAILURE") {
      message = "failed"
      color = "danger"
    }
    slackSend message: "Build ${message} - ${env.JOB_NAME} ${env.BUILD_NUMBER} (<${env.BUILD_URL}/console|Open>)", color: color

  if (err) {
    // throw error again to propagate build failure.
    // Otherwise Jenkins thinks that the pipeline completed successfully.
    throw err
  }
}
