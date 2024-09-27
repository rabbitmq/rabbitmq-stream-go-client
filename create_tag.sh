#/bin/bash

version=$1
gpg_key=$2
regex="^([0-9]+)\.([0-9]+)\.([0-9]+)(-(alpha|beta|rc)\.[0-9]+)?$"
tag="v$version"

if [ $# -lt 2 ]; then
    echo "Usage: $0 <version> <gpg_key>"
    exit 1
fi

if [[ ! $version =~ $regex ]]; then
    echo "Invalid version format: $version"
    exit 1
fi

echo "Updating version and constants to $version"
echo $version > VERSION
sed -i -e "s/.*ClientVersion = \"*.*/ClientVersion = \"$version\"/" pkg/stream/constants.go
go fmt ./...

echo ""
echo "Committing changes"
git add VERSION pkg/stream/constants.go README.md
git commit -m "rabbitmq-stream-go-client $tag"

echo ""
echo "Creating and pushing tag $tag"
git tag -a -s -u $gpg_key -m "rabbitmq-stream-go-client $tag" $tag && git push && git push --tags
