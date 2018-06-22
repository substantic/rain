
cd `dirname $0`

npm run build

rm -rf dist
mkdir dist

cp build/index.html dist/index.html
cp build/static/js/main*.js dist/main.js
cp build/static/css/main*.css dist/main.css

gzip dist/main.js
gzip dist/main.css

mkdir -p ../rain_server/dashboard/dist
mv dist/main.js.gz ../rain_server/dashboard/dist/main.js.gz
mv dist/main.css.gz ../rain_server/dashboard/dist/main.css.gz
mv dist/index.html ../rain_server/dashboard/dist/index.html
