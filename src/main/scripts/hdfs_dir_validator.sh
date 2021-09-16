# sh hdfs_dir_validator.sh /user/training/surender_hadoop/input_files/emp.txt

HDFS_LOC=$1

echo "HDFS LOCATION : $HDFS_LOC"

hdfs dfs -test -e $HDFS_LOC

if [ $? -eq 0 ]
then
        echo "$HDFS_LOC exists"
else
        echo "$HDFS_LOC not exists"
fi



if hdfs dfs -test -e $HDFS_LOC
then
        echo "$HDFS_LOC exists"
else
        echo "$HDFS_LOC not exists"
fi


if $(hdfs dfs -test -e $HDFS_LOC)
then
       echo "ok"
else
       echo "not ok"
fi
