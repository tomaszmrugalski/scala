// Projekt 2 z Kosmicznych Zastosowañ Zaawansowanych Technologii Informatycznych
// zwany dla uproszczenia HPC.
//
// Autorzy: Tomasz Mrugalski, Dawid Czubak
//
// Zadanie 2
//
// 1. wygenerowac RDD tekstowe: names, 64 elementy, kazdy element imienia jednej osoby z zespolu, 70% osoby starsza, 30% osoba mlodsza
// 2. stworzyc 2 zbiory RDD klucz-wartosc:
//  - len (dlugosci elementow z names)
//  - sum (iloczyny wartosci unicode elementow names (float)
// 3. obliczyc statsy
// 4. znalezc unikatowe elementy we wszysktich RDD
// 5. zapisac wszysktie RDD na dysk
//
// Sprawko do 11 grudnia
// 1. kod zrodlowe
// 2. wyniki dzialania programow
// 3. liste elementow names
// 4. histogramy dla wszystkich RDD

import scala.util.Random
import java.io._

println("# HPC projekt 2, autorzy: Tomasz Mrugalski, Dawid Czubak")

// ZADANIE 1: wygenerowac RRD tekstowe z 64 elementami

// @todo: allowedValues powinno byc generowane automatycznie, bo dla proporcji 70/30 jest ³atwo - wystarczy tylko 10
// elementow. Gdyby proporcje byly inne (np. 49.9/50.1), trzeba byloby podac 1000 elementow.

val allowedValues = Seq ( "Tomasz", "Tomasz", "Tomasz", "Tomasz", "Tomasz", "Tomasz", "Tomasz", "Dawid", "Dawid", "Dawid" )

val random = new Random

// Wykonaj w petli 64 razy: zwroc losowa wartosc elementu z tablicy allowedValues
val names = for (i <- 0 to 63) yield allowedValues(random.nextInt(allowedValues.length))

// ZADANIE 2: Stworzyc 2 zbiory RDD klucz-wartosc

// Teraz oblicz dlugosci dla kazdego stringa
// To zwrociloby same dlugosci: var lengths = names.map(x => x.length)

// lengths to lista par. Kazda para sklada sie ze stringa z imieniem i jego dlugoscia, np. (Dawid, 5)
val len_sequence = names.map(x => (x,x.length))

var len = sc.parallelize(len_sequence)

// Takie cos oblicza iloczyn znakow w stringu. Jest tu pare problemow.
// Zwykly int zapisywany jest na 4 bajtach. Zatem kazdy string o dlugosci wiekszej niz 4 znaki ma potencjal do tego,
// zeby przekrecic zakres inta. Dlatego lepiej operacje wykonac na double'u.
// "Tomek" - zwykly string
// "Tomek".map(_.toByte) - wektor bajtow: Vector(84, 111, 109, 101, 107)
// "Tomek".map(_.toByte.toFloat) - wektor floatow: Vector(84.0, 111.0, 109.0, 101.0, 107.0)
// "Tomek".map(_.toByte.toFloat).reduce(_ * _) - redukcja, ktora bierze poprzedni element (pierwsze _) i mnozy razy kolejny  (drugie _)
var sum_sequence = names.map(x => (x,x.map(_.toByte.toFloat).reduce(_ * _)))

var sum = sc.parallelize(sum_sequence)

// ZADANIE 3: Obliczyc statystyki
// Probowalem zdefiniowac metode wypisujaca statystyki, ale names, len i sum maja zupelnie inne typy

// Skoro mamy wypisac statystyki dla dwoch zbiorow RDD, to wygodnie jest napisac do tego funkcje.
// Przyjmuje ona 2 parametry: nazwe RDD (zeby napisac, czego dotycza statystyki) oraz wartosci rdd
// len ma wartosci int, natomiast sum ma float. Dlatego tutaj bedziemy operowac na floatach.

def printStats(name: String, rdd: org.apache.spark.rdd.RDD[(Float)]) : Unit = {
    val count = rdd.count
    val mean = rdd.sum * 1.0 / count
    val devs = rdd.map(score => (score - mean) * (score - mean))
    val stddev = Math.sqrt(devs.sum / (count - 1))

   // zamiast rdd.reduce(_ min _) mozna uzyc zapisu skrotowego: rdd.min

    printf("--- %s stats ---\n", name)
    printf("Length    = %d\n", rdd.count)
    printf("average   = %f\n", rdd.reduce(_ + _) * 1.0 / rdd.count)
    printf("min value = %f\n", rdd.reduce(_ min _))
    printf("max value = %f\n", rdd.reduce(_ max _))
    printf("std dev   = %f\n", stddev)
}

// Tu jest niezly cyrk. len.map(_._2) zwroci liste samych dlugosci. .reduce(_ + _) doda wszystkie
// elementy razem, * 1.0 skonwertuje na floata, a potem podzielimy przez dlugosc tablicy (czyli
// ilosc elementow).
// printf("average   = %f\n", len.map(_._2).reduce(_ + _) * 1.0 / len.length)

// Wersja kanoniczna:
// printf("min value = %d\n", len.map(_._2).reduce(_ min _))
// printf("max value = %d\n", len.map(_._2).reduce(_ max _))
// Wersja krotka:
// printf("min value = %d\n", len.map(_._2).min)

val tmp = len.map(_._2.toFloat)
printStats("len", len.map(_._2.toFloat))

printStats("sum", sum.map(_._2))

// ZADANIE 4: Znalezc unikatowe elementy we wszystkich RDD
printf("--- unique values ---\n")

// Unikatowe elementy w names, len i sum
printf("Unique names  =")
sc.parallelize(names).distinct().collect


printf("Unique len    =")
len.distinct().collect

printf("Unique sum    =")
sum.distinct().collect


// ZADANIE 5: zapisac wszystkie RDD na dysk

