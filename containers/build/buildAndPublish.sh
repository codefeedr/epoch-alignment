docker login nvankaam
rm -rf ivycache
cp -R /mnt/c/Users/Niels/.ivy2 ivycache/
docker build . --tag nvankaam/codefeedr_build:latest
docker push nvankaam/codefeedr_build


#docker ps -aq --no-trunc -f status=exited | xargs docker rm