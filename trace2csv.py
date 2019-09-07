# open.py

import sys
import re
import fileinput


#def main(files):
def main():

    match_counter = 0
    progress = 0

    exesta = re.compile("EXECUTE_STATEMENT_FINISH")
    opthom = re.compile("(\/opt[^:]+)|(\/home[^:]+)")
    statem = re.compile("Statement")
    tables = re.compile("Table")
    recfet = re.compile("\d+(?= records fetched)")
    planre = re.compile("PLAN")
    crtice = re.compile("------")
    zvjezd = re.compile("\*{5,}")
    kapice = re.compile("\^\^\^")
    rekaps = re.compile("(\d+(?= ms))\D+(\d+(?= read))?\D+(\d+(?= write))?\D+(\d+(?= fetch))?\D+(\d+(?= mark))?")

    t_stamp = ""
    launch = ""
    plan = ""
    full_SQL = ""
    fetched = 0

    o = open("output","w")
    t = open("tables","w")
     
     
    for line in fileinput.input():
          
        #pocetak ciklusa
        if (progress ==0):
            if (exesta.search(line)):
                progress += 1
                t_stamp = line[:24]
            continue

        #launch - tko je pokrenuo sql
        if (progress == 1):
            oh = opthom.search(line)
            if (oh):
                launch = oh.group(0)
                progress += 1
            elif (statem.match(line)):
                launch = "internal"
                progress += 2
            continue

        #statement
        if (progress == 2):
            if (statem.match(line)):
                progress += 1
            continue

        #SQL statement
        if (progress == 3):
            rf = recfet.match(line)
            if (rf):
                plan = "error"
                fetched = rf.group(0)
                progress += 3
            elif (planre.match(line)):
                plan = line
                progress += 1
            elif (crtice.match(line)):
                pass
            elif (kapice.match(line)):
                progress += 1
            else:
                full_SQL += re.sub("\s+", " ", line)
            continue

        #PLAN
        if (progress == 4):
            if (planre.match(line)):
                if (plan != ""): plan += " && "
                plan += re.sub("\s+"," ", line)
                continue
            else:
                if (plan == ""): plan = "error"
                progress += 1

        #fetched
        if (progress == 5):
            rf = recfet.match(line)
            if (rf):
                fetched = rf.group(0)
                progress += 1
            continue

        #rekapitulacija - trajanje i kolicina operacija
        if (progress == 6):
            ru = rekaps.findall(line)
            progress += 1
            continue

        #tablice i broj operacija po tablici
        if (progress == 7):
            if (tables.match(line)): progress += 1
            continue

        if (progress == 8):
            if (zvjezd.match(line)): progress += 1
            continue
        
        if (progress == 9):
            if (line == "\n"):
                progress += 1
            else:
                tableout = [line[0:31].rstrip(),  line[31:41].lstrip(), \
                            line[41:51].lstrip(), line[51:61].lstrip(), \
                            line[61:71].lstrip(), line[71:81].lstrip(), \
                            line[81:91].lstrip(), line[91:101].lstrip(), \
                            line[101:111].lstrip()]
                for r in range(1,9):
                    if tableout[r] == '': tableout[r] = '0'
                s = ";".join(tableout)
                t.write(s + '\n')
                continue



        #END
        if (progress > 9):   
            lineout = [t_stamp, launch, full_SQL, plan, fetched]
            for r in ru[0]:
                if r == '': r = '0'
                lineout.append(r)
            s = ";".join(lineout)
            o.write(s + '\n')
            progress = 0
            full_SQL = ""
            plan = ""
            fetched = 0

    fileinput.close()
    o.close()
    t.close()


if __name__ == "__main__":
    main ()




