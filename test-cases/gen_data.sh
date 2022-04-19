for ((i=0; i<100000; ++i)) do
    echo -n $(shuf -i0-16 -n2)
    echo -n " "
    echo -n $(shuf -i0-65536 -n200 -r)
    echo
done >> big_data.csv
