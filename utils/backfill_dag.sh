d=2020-01-01
while [ "$d" != 2020-06-20 ]; do
  d2=$(date -I -d "$d + 1 day")
  airflow backfill RxDocBackfill -s $d -e $d2 --rerun_failed_tasks
  d=$(date -I -d "$d + 1 day")
done
