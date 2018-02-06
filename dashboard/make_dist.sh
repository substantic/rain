
cd `dirname $0`

npm run build

rm -rf dist
mkdir dist

cp build/index.html dist/index.html
cp build/static/js/main*.js dist/main.js
cp build/static/css/main*.css dist/main.css

gzip dist/main.js
gzip dist/main.css
