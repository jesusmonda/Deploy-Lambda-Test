pipeline {
    agent any

    environment { 
        ENVIRONMENT = "${BRANCH_NAME == "master" ? "pro" : "dev"}"
    }
    options {
        timeout(time: 10, unit: 'MINUTES') 
    }

    stages {
        stage('Build') {
            agent any
            options {
                timeout(time: 1, unit: 'MINUTES') 
            }
            steps {
                sh '''
                    changed_files=`git diff-tree --name-only --no-commit-id ${GIT_COMMIT}`
                    touch changed_files.txt
                    echo $changed_files > changed_files.txt

                    for x in $changed_files; do
                        if [ -d "$x" ]; then
                            echo "$x is a directory."
                            zip -r -j $x.zip $x/* > /dev/null
                        fi
                    done;
                '''
            }
        }
        stage('Publish new version') {
            agent any
            options {
                timeout(time: 1, unit: 'MINUTES') 
            }
            steps {
                sh '''
                    changed_files=`cat changed_files.txt`
                    for x in $changed_files; do
                        if [ -d "$x" ]; then
                            echo "Publish new version $x"
                            aws lambda update-function-code --function-name $x --zip-file $x.zip
                            echo `aws lambda publish-version --function-name $x --query 'Version'` > version_$x.txt
                        fi
                    done;
                '''
            }
        }
        stage('Test') {
            agent any
            options {
                timeout(time: 1, unit: 'MINUTES') 
            }
            steps {
                sh '''
                    // Test
                    changed_files=`cat changed_files.txt`
                    for x in $changed_files; do
                        if [ -d "$x" ]; then
                            echo "Testing $x"
                            version=`cat version_$x.txt`
                            aws lambda invoke --function-name $x:$version --payload '{ "key": "value" }' // Todo test payload
                        fi
                    done;
                '''
            }
        }
        stage('Deploy') {
            agent any
            options {
                timeout(time: 1, unit: 'MINUTES') 
            }
            steps {
                sh '''
                    changed_files=`cat changed_files.txt`
                    for x in $changed_files; do
                        if [ -d "$x" ]; then
                            echo "Deploying $x"
                            version=`cat version_$x.txt`
                            aws lambda update-alias --function-name $x --name $ENVIRONMENT --function-version $version
                        fi
                    done;
                '''
            }
        }
    }

    post {
        failure {
            echo 'Fail'
        }
    }
}
