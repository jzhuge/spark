#!/bin/bash

usage() {
  cat <<EOF
assume-role.sh [-h|--help] [-a|--arn <arn>] [-s|--session-name <session_name>] [--vault] [cmd ...]
EOF
}

while (($# > 0)); do
  case $1 in
  -h|--help)
    usage
    exit 0
    ;;
  -a|--arn)
    arn=$2
    shift
    ;;
  -s|--session-name)
    session_name=$2
    shift
    ;;
  --vault)
    arn="arn:aws:iam::219382154434:role/s3_all_with_vault"
    ;;
  -*)
    echo "Unknown option $1" >&2
    exit 1
    ;;
  *)
    break
  esac
  shift
done

export BUILD_CREDENTIALS=$( \
  aws sts assume-role \
    --role-arn arn:aws:iam::219382154434:role/BDP_JENKINS_ROLE \
    --role-session-name $USER \
  | jq '.Credentials')

export AWS_ACCESS_KEY_ID=$(jq -r '.AccessKeyId' <<<$BUILD_CREDENTIALS)
export AWS_SECRET_ACCESS_KEY=$(jq -r '.SecretAccessKey' <<<$BUILD_CREDENTIALS)
export AWS_SESSION_TOKEN=$(jq -r '.SessionToken' <<<$BUILD_CREDENTIALS)

exec "$@"
