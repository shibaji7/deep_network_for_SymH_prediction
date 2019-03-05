scp -r shibaji7@newriver1.arc.vt.edu:~/deep_network_for_SymH_prediction .

cd deep_network_for_SymH_prediction
git lfs track "*.asc"
git lfs track "*.h5"

git add --all

git commit -m ""
git push

cd
rm -rf deep_network_for_SymH_prediction
