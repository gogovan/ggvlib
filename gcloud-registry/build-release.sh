#!/usr/bin/env bash
VERSION=$(poetry version | egrep -o '[0-9.]+')
RELEASE_PATH=./dist/*$VERSION*.tar.gz
poetry build
echo "Creating release for $RELEASE_PATH"
gh release create $VERSION $RELEASE_PATH \
--notes $VERSION \
--title common \