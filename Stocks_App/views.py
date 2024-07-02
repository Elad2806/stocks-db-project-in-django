from django.shortcuts import render
from django.db import connection
from datetime import datetime
from .models import *
def dictfetchall(cursor):
    "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

def index(request):

    return render(request, 'index.html')

def query_results(request):
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT BuyingGB.Symbol, Investor.Name, BuyingGB.NumOfStocks
            FROM PopularCompany, BuyingGB, Investor,
                 (SELECT BuyingGB.Symbol, MAX(NumOfStocks) as MaxStocks
                  FROM BuyingGB
                  GROUP BY Symbol) as MaxStocksByCompany
            
            WHERE BuyingGB.Symbol = MaxStocksByCompany.Symbol AND MaxStocksByCompany.MaxStocks = BuyingGB.NumOfStocks
              AND PopularCompany.Symbol = BuyingGB.Symbol AND NumOfStocks > 10 AND BuyingGB.ID = Investor.ID
            ORDER BY Symbol ASC, Name ASC;
        """)
        sql_res1 = dictfetchall(cursor)


    return render(request, 'query_results.html',{'sql_res1': sql_res1})


def add_transaction(request):
    with connection.cursor() as cursor:
        if request.method == 'POST' and request.POST:
            iID = request.POST["ID"]
            quantity = request.POST["TQuantity"]
            today = datetime.today().strftime('%Y-%m-%d')

            cursor.execute("""
                        SELECT ID
                        FROM Investor
                        WHERE ID =%s""", [iID])
            sql_res3 = dictfetchall(cursor)
            if len(sql_res3) == 0:
                sql_res3 = {'Error: ID does not exist'}
            else:
                updated_investor = Investor.objects.get(id=iID)
                try:
                    old_transaction = Transactions.objects.get(id=updated_investor, tdate=today)
                    updated_investor.availablecash -= old_transaction.tquantity
                    updated_investor.save()
                    Transactions.objects.filter(id=iID, tdate=today).delete()

                except Transactions.DoesNotExist:
                    pass

                finally:
                    Transactions.objects.create(id=updated_investor, tquantity=quantity, tdate=today)
                    updated_investor.availablecash += int(quantity)
                    updated_investor.save()
                    sql_res3 = None

        else:
            sql_res3 = None

        cursor.execute("""
                        SELECT TOP 10 *
                        FROM Transactions
                        ORDER BY tDate DESC, ID DESC;""")
        sql_res4 = dictfetchall(cursor)


    return render(request, 'add_transaction.html', {'sql_res3': sql_res3,
                                                    'sql_res4': sql_res4})


def buy_stocks(request):
    with connection.cursor() as cursor:

        cursor.execute("""
                        SELECT TOP 10 *
                        FROM Payed
                        ORDER BY Payed DESC, ID DESC;""")
        sql_res5 = dictfetchall(cursor)

        if request.method == 'POST' and request.POST:
            iID = request.POST["ID"]
            company = request.POST["Symbol"]
            quantity = request.POST["BQuantity"]
            today = datetime.today().strftime('%Y-%m-%d')
            sql_res3 = {}

            try:
                updated_investor = Investor.objects.get(id=iID)

            except Investor.DoesNotExist:
                sql_res3 = {'Error: ID does not exist':1}

            try:
                updated_company = Company.objects.get(symbol=company)

            except Company.DoesNotExist:
                sql_res3.update({'Error: Company symbol does not exist':2})

            if len(sql_res3) > 0:
                return render(request, 'buy_stocks.html', {'sql_res3': sql_res3,
                                                           'sql_res5': sql_res5})

            cursor.execute("""
                SELECT Symbol, Price
                FROM LastPrice
                WHERE Symbol = %s""", [company])
            res = dictfetchall(cursor)
            last_price = res[0].get('Price')

            new_stock, created = Stock.objects.get_or_create(symbol=updated_company, tdate=today, price=last_price)

            cursor.execute("""
            SELECT *
            FROM Buying
            WHERE tDate = %s AND ID = %s AND Symbol = %s""", [today,iID,company])
            res = dictfetchall(cursor)
            if len(res) > 0:
                sql_res3 = {'Error: This user has already bought stocks of this company today'}
                return render(request, 'buy_stocks.html', {'sql_res3': sql_res3,
                                                           'sql_res5': sql_res5})
            else:
                cursor.execute("""
INSERT INTO Buying
VALUES (%s, %s, %s, %s);""",[today,iID,company,quantity])

            cursor.execute("""
                SELECT AvailableCash
                FROM Investor
                WHERE ID = %s""", [iID])
            res2 = dictfetchall(cursor)
            cash_available = res2[0].get('AvailableCash')

            payed = int(int(last_price) * int(quantity))

            if payed > int(cash_available):
                sql_res3 = {'Error: Investor does not have enough cash'}
                return render(request, 'buy_stocks.html', {'sql_res3': sql_res3,
                                                           'sql_res5': sql_res5})
            updated_investor.availablecash -= payed
            updated_investor.save()

        else:
            sql_res3 = None

        cursor.execute("""
                        SELECT TOP 10 *
                        FROM Payed
                        ORDER BY Payed DESC, ID DESC;""")
        sql_res5 = dictfetchall(cursor)

    return render(request, 'buy_stocks.html',{'sql_res3': sql_res3,
                                              'sql_res5': sql_res5})