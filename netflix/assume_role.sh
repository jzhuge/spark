#!/bin/bash

export BUILD_CREDENTIALS=$( \
  aws sts assume-role \
    --role-arn arn:aws:iam::219382154434:role/BDP_JENKINS_ROLE \
    --role-session-name $USER \
  | jq '.Credentials')

export AWS_ACCESS_KEY_ID=$(jq -r '.AccessKeyId' <<<$BUILD_CREDENTIALS)
export AWS_SECRET_ACCESS_KEY=$(jq -r '.SecretAccessKey' <<<$BUILD_CREDENTIALS)
export AWS_SESSION_TOKEN=$(jq -r '.SessionToken' <<<$BUILD_CREDENTIALS)

exec "$@"
