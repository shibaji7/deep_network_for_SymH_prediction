cp -r ~/DATA ~/deep_network_for_SymH_prediction
cd deep_network_for_SymH_prediction
git lfs track "*.asc"
git lfs track "*.h5"

git add --all

git commit -m ""
git push

cd
