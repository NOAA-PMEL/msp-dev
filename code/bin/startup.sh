#/bin/bash
case $1 in
    init)
    python bin/envds.py startup services -id clear -f $(pwd)/runtime/clear/services.txt
esac
# fi
# echo $1
# if arg = "init";
#     then echo "init";
# elif $1 = "interface";
#     then echo "interface;
# fi