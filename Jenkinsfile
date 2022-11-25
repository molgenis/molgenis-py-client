pipeline {
    agent {
        kubernetes {
            // the shared pod template defined on the Jenkins server config
            inheritFrom 'shared'
            // pod template defined in molgenis/molgenis-jenkins-pipeline repository
            yaml libraryResource("pod-templates/python.yaml")
        }
    }
    stages {
        stage('Prepare') {
            steps {
                script {
                    env.GIT_COMMIT = sh(script: 'git rev-parse HEAD', returnStdout: true).trim()
                }
                container('vault') {
                    script {
                        env.PYPI_REGISTRY = sh(script: 'vault read -field=registry secret/ops/account/pypi', returnStdout: true)
                        env.PYPI_USERNAME = sh(script: 'vault read -field=username secret/ops/account/pypi', returnStdout: true)
                        env.PYPI_PASSWORD = sh(script: 'vault read -field=password secret/ops/account/pypi', returnStdout: true)
                        env.GITHUB_TOKEN = sh(script: 'vault read -field=value secret/ops/token/github', returnStdout: true)
                        env.SONAR_TOKEN = sh(script: 'vault read -field=value secret/ops/token/sonar', returnStdout: true)
                        env.CI_PASSWORD = sh(script: 'vault read -field=password secret/dev/account/master.dev.molgenis.org', returnStdout: true)
                        env.CI_HOST = 'https://master.dev.molgenis.org'
                    }
                }
                container('python') {
                    script {
                        sh "pip install --upgrade pip"
                        sh "pip install bumpversion"
                        sh "pip install twine"
                    }
                }
            }
        }
        stage('Build: [ pull request ]') {
            when {
                changeRequest()
            }
            steps {
                container('python') {
                    sh "python setup.py test"
                    sh "pip install ."
                }
                container('sonar') {
                    sh "sonar-scanner -Dsonar.github.oauth=${env.GITHUB_TOKEN} -Dsonar.pullrequest.base=${CHANGE_TARGET} -Dsonar.pullrequest.branch=${BRANCH_NAME} -Dsonar.pullrequest.key=${env.CHANGE_ID} -Dsonar.pullrequest.provider=GitHub -Dsonar.pullrequest.github.repository=molgenis/molgenis-py-client"
                }
            }
        }
        stage('Build: [ master ]') {
            when {
                branch 'master'
            }
            steps {
                milestone 1
                container('python') {
                    sh "python setup.py test"
                    sh "pip install ."
                }
                container('sonar') {
                    sh "sonar-scanner"
                }
            }
        }
        stage('Release: [ master ]') {
            when {
                branch 'master'
            }
            environment {
                REPOSITORY = 'molgenis/molgenis-py-client'
            }
            steps {
                timeout(time: 30, unit: 'MINUTES') {
                    script {
                        env.RELEASE_SCOPE = input(
                                message: 'Do you want to release?',
                                ok: 'Release',
                                parameters: [
                                        choice(choices: 'patch\nminor\nmajor', description: '', name: 'RELEASE_SCOPE')
                                ]
                        )
                    }
                }
                milestone 2
                container('python') {
                    sh "git remote set-url origin https://${GITHUB_TOKEN}@github.com/${REPOSITORY}.git"

                    sh "git checkout -f ${BRANCH_NAME}"
                    
                    sh "pip install bumpversion"

                    sh "bumpversion ${RELEASE_SCOPE} setup.py"

                    script {
                        env.PYTHON_CLIENT_VERSION = sh(script: "grep current_version .bumpversion.cfg | cut -d'=' -f2", returnStdout: true).trim()
                    }

                    sh "python setup.py sdist bdist_wheel"

                    sh "twine upload --repository-url ${PYPI_REGISTRY} -u ${PYPI_USERNAME} -p ${PYPI_PASSWORD} dist/*"

                    sh "git push --tags origin master"
                    molgenisSlack(message: "Python REST Client ${PYTHON_CLIENT_VERSION} has been successfully released! :tada: https://pypi.org/project/molgenis-py-client/", status:'SUCCESS', channel: "#pr-platform")
                }
            }
        }
    }
    post{
        success {
            notifySuccess()
        }
        failure {
            notifyFailed()
        }
    }
}

def notifySuccess() {
    molgenisSlack(message: 'Build success', status:'INFO', site: '#pr-platform')
}

def notifyFailed() {
    molgenisSlack(message: 'Build failed', status:'ERROR', site: '#pr-platform')
}
