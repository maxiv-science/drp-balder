FROM python:3

WORKDIR /tmp

ARG CI_COMMIT_SHA=0000
ARG CI_COMMIT_REF_NAME=none
ARG CI_COMMIT_TIMESTAMP=0
ARG CI_PROJECT_URL=none

COPY requirements.txt /tmp/requirements.txt

RUN python -m pip --no-cache-dir install -r requirements.txt

COPY . /tmp

COPY <<EOF /etc/build_git_meta.json
{
"commit_hash": "${CI_COMMIT_SHA}",
"branch_name": "${CI_COMMIT_REF_NAME}",
"timestamp": "${CI_COMMIT_TIMESTAMP}",
"repository_url": "${CI_PROJECT_URL}"
}
EOF


CMD ["dranspose"]
