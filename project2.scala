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

val allowedValues = Seq ( "Tomasz", "Tomasz", "Tomasz", "Tomasz", "Tomasz", "Tomasz", "Tomasz", "Dawid", "Dawid", "Dawid" )

val random = new Random

// Wykonaj w petli 64 razy: zwroc losowa wartosc elementu z tablicy allowedValues
val names = for (i <- 0 to 63) yield allowedValues(random.nextInt(allowedValues.length))

// Teraz oblicz dlugosci dla kazdego stringa
// To zwrociloby same dlugosci: var lengths = names.map(x => x.length)

// lengths to lista par. Kazda para sklada sie ze stringa z imieniem i jego dlugoscia, np. (Dawid, 5)
val lengths = names.map(x => (x,x.length))

// Takie cos oblicza iloczyn znakow w stringu. Jest tu pare problemow.
// Zwykly int zapisywany jest na 4 bajtach. Zatem kazdy string o dlugosci wiekszej niz 4 znaki ma potencjal do tego,
// zeby przekrecic zakres inta. Dlatego lepiej operacje wykonac na double'u.
// "Tomek" - zwykly string
// "Tomek".map(_.toByte) - wektor bajtow: Vector(84, 111, 109, 101, 107)
// "Tomek".map(_.toByte.toFloat) - wektor floatow: Vector(84.0, 111.0, 109.0, 101.0, 107.0)
// "Tomek".map(_.toByte.toFloat).reduce(_ * _) - redukcja, ktora bierze poprzedni element (pierwsze _) i mnozy razy kolejny  (drugie _)
var multi = names.map(x => (x,x.map(_.toByte.toFloat).reduce(_ * _)))
