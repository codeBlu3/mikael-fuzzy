for file in *.py ; do
    vim +':w ++ff=unix' +':q' "${file}"
done
    
