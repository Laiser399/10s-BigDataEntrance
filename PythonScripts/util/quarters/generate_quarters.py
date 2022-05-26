import csv

start_year, start_quarter = 2008, 3
end_year, end_quarter = 2021, 4

with open('output/quarters.csv', 'w', newline='') as output_file:
    writer = csv.writer(output_file, delimiter='\t', escapechar='\\')
    y, q = start_year, start_quarter
    while True:
        if y > end_year:
            break
        if y == end_year and q > end_quarter:
            break

        writer.writerow((y, q))

        q += 1
        if q == 5:
            y += 1
            q = 1
