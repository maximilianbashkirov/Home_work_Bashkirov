#!/bin/bash

if [ $# -ne 1 ]; then
    echo "Ошибка: необходимо указать имя файла с данными" >&2
    exit 1
fi
file="$1"
if [ ! -f "$file" ]; then
    echo "Ошибка: файл '$file' не найден" >&2
    exit 1
fi

awk '
BEGIN {
    total = 0
}
{
    if (NF == 0) next

    date = $1
    weekday = $2
    product = $3
    price = $4
    qty = $5
    sale = price * qty
    total += sale
    day_total[date] += sale
    if (!(date in day_week)) {
        day_week[date] = weekday
    }
    prod_qty[product] += qty
    prod_sum[product] += sale
}
END {
    printf "Общая сумма продаж: %.2f\n", total
    max_day = -1
    max_day_date = ""
    for (d in day_total) {
        if (day_total[d] > max_day) {
            max_day = day_total[d]
            max_day_date = d
        }
    }
    if (max_day_date != "") {
        printf "День с наибольшей выручкой: %s %s (сумма продаж: %.2f)\n", max_day_date, day_week[max_day_date], max_day
    }

    max_qty = -1
    max_prod = ""
    for (p in prod_qty) {
        if (prod_qty[p] > max_qty) {
            max_qty = prod_qty[p]
            max_prod = p
        }
    }
    if (max_prod != "") {
        printf "Популярный товар: %s (количество проданных единиц: %.0f, сумма продаж: %.2f)\n", max_prod, max_qty, prod_sum[max_prod]
    }
}
' "$file"

