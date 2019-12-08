// Projekt 2 z Kosmicznych Zastosowañ Zaawansowanych Technologii Informatycznych
// zwany dla uproszczenia HPC.
//
// Autorzy: Tomasz Mrugalski, Dawid Czubak
//
// Zadanie 2
//
// 1. wygenerowac RDD tekstowe: names, 64 elementy, kazdy element imienia jednej osoby z zespolu, 70% osoby starsza, 30% osoba mlodsza
// 2. stworzy 2 zbiory RDD klucz-wartosc:
//  - len (dlugosci elementow z names)
//  - sum (iloczyny wartosci unicode elementow names (float)
// 3. obliczyc statsy
// 4. znalezc unikatowe elementy we weszysktich RDD
// 5. zapisac wszysktie RDD na dysk
//
// Sprawko do 11 grudnia
// 1. kod zrodlowe
// 2. wyniki dzialania programow
// 3. liste elementow names
// 4. histogramy dla wszystkich RDD

import scala.util.Random

println("# HPC projekt 2, autorzy: Tomasz Mrugalski, Dawid Czubak")


// @todo: allowedValues powinno byc generowane automatycznie, bo dla proporcji 70/30 jest ³atwo - wystarczy tylko 10
// elementow. Gdyby proporcje byly inne (np. 49.9/50.1), trzeba byloby podac 1000 elementow.

val allowedValues = Seq ( "Tomek", "Tomek", "Tomek", "Tomek", "Tomek", "Tomek", "Tomek", "Dawid", "Dawid", "Dawid" )
val random = new Random

// Wykonaj w petli 64 razy: zwroc losowa wartosc elementu z tablicy allowedValues
var rdd1 = for (i <- 0 to 63) yield allowedValues(random.nextInt(allowedValues.length))
