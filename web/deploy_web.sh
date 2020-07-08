cd $(git rev-parse --show-toplevel)
cd web
npm install 
yarn build
cd $(git rev-parse --show-toplevel)
git subtree push --prefix Web origin web