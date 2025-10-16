# Zestaw 21 – parks-visits

## Data

Dane sztuczne wygenerowane z użyciem biblioteki DataFaker (https://www.datafaker.net/)

### Dwa zbiory danych

#### `datasource1` – wizyty w parkach (visits)

Dane mają format CSV, pliki posiadają nagłówek.

Pola w pliku:

0. `visit_id` – unikalny identyfikator wizyty (UUID)  
1. `park_id` – identyfikator parku (odniesienie do słownika parków)  
2. `visitor_id` – unikalny identyfikator odwiedzającego (np. VISITOR0000001)  
3. `entry_time` – czas wejścia (`yyyy-MM-dd'T'HH:mm`)  
4. `exit_time` – czas wyjścia (`yyyy-MM-dd'T'HH:mm`)  
5. `group_size` – liczba osób w grupie  
6. `activity` – aktywność podczas wizyty (np. Hiking, Picnic, Camping, Birdwatching)  

---

#### `datasource4` – dane na temat parków (parks)

Dane mają format CSV, pliki posiadają nagłówek.

Pola w pliku:

0. `park_id` – unikalny identyfikator parku (np. PARK0001)  
1. `name` – nazwa parku  
2. `region` – region geograficzny (North, South, East, West, Central)  
3. `facilities` – dostępne udogodnienia (lista rozdzielana średnikiem, np. Playground;Picnic Area)  
4. `attractions` – atrakcje w parku (lista rozdzielana średnikiem, np. Lake;Monument)  
5. `nature_types` – typy przyrody (lista rozdzielana średnikiem, np. Forest;River)  
6. `established_year` – rok utworzenia parku  

## Program MapReduce (2)

Działając na zbiorze `datasource1` `(1)` należy obliczyć statystyki wizyt dla każdego parku w poszczególnych dniach.  

Dane powinny zostać pogrupowane według:  
* identyfikatora parku (`park_id`),  
* daty wizyty (`date`).  

W ramach każdej grupy należy obliczyć:  
* `visits_count` – łączną liczbę wizyt w danym parku w danym dniu,  
* `avg_group_size` – średnią liczebność grup odwiedzających.  

W wynikowym zbiorze `(3)` powinny znaleźć się następujące atrybuty:  
* `park_id` – identyfikator parku,  
* `date` – data wizyty (dzień),  
* `visits_count` – liczba wizyt w danym dniu,  
* `avg_group_size` – średnia wielkość grup odwiedzających w danym dniu.  

## Running

1. Start cluster (cloud shell)
```shell
gcloud dataproc clusters create ${CLUSTER_NAME} --enable-component-gateway \
--region ${REGION} --subnet default --public-ip-address \
--master-machine-type n2-standard-4 --master-boot-disk-size 50 \
--num-workers 2 --worker-machine-type n2-standard-2 --worker-boot-disk-size 50 \
--image-version 2.2-debian12 \
--project ${PROJECT_ID} --max-age=1h
```
2. Move data to HDFS 
```shell
hadoop fs -mkdir -p input
export BUCKET_NAME=<bucket_name>
hadoop fs -cp <park_visits_dataset/input> gs://$BUCKET_NAME/ParkVisits/input/
hadoop fs -mkdir -p gs://$BUCKET_NAME/ParkVisits/output/
```
3. Upload MapReduce program .jar file to hadoop master
4. Run job (master machine)
```shell
hadoop jar parkvisits.jar gs://$BUCKET_NAME/ParkVisits/input/datasource1  gs://$BUCKET_NAME/ParkVisits/output/
```

---

## Program Hive (5)

Działając na wyniku zadania MapReduce `(3)` oraz zbiorze danych `datasource4` `(4)` należy powiązać dane o wizytach ze słownikiem parków.  

Na podstawie tych danych należy wyliczyć, dla każdego typu przyrody (`nature_types`) i regionu:  
* łączną liczbę wizyt (`total_visits`),  
* średnią wielkość grup (`avg_group_size`).  

Ponadto należy obliczyć, dla każdego typu przyrody, odchylenie liczby wizyt w danym regionie od średniej liczby wizyt we wszystkich regionach (`visits_deviation`).  

Wynik `(6)` powinien zawierać następujące atrybuty:  
* `nature_type` – typ przyrody (np. Forest, Desert, Coast),  
* `region` – region parku,  
* `total_visits` – łączna liczba wizyt w parkach tego typu w regionie,  
* `avg_group_size` – średnia wielkość grup w parkach tego typu w regionie,  
* `visits_deviation` – różnica między liczbą wizyt w regionie a średnią liczbą wizyt dla danego typu przyrody we wszystkich regionach.  

---

Cyfry w nawiasach odnoszą się do cyfr wykorzystanych na graficznej reprezentacji projektu – patrz opis projektu na stronie kursu.
