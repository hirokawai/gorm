dialects=("postgres" "mysql")

for dialect in "${dialects[@]}" ; do
    DEBUG=false GORM_DIALECT=${dialect} go test
done
