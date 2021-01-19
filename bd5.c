#include <sqlca.h>
#include <stdio.h>
#include <stdlib.h>

exec sql begin declare section;
const char q1[] = "select round (avg(a.count_post), 2) sr from (select count(*) count_post from spj group by spj.n_post) a";
const char q2[] = "select a.n_izd, j.name, a.n_det, p.name, p.cvet, a.sum_kol from(select spj.n_izd, spj.n_det, sum(spj.kol) sum_kol from spj join j on j.n_izd = spj.n_izd where j.town = ? group by spj.n_det, spj.n_izd)a join j on a.n_izd = j.n_izd join p on a.n_det = p.n_det order by a.n_izd, a.n_det;";
const char q3[] = "select a.town, a.count_post, b.total_post, round(a.count_post*100.0/b.total_post, 2) proc from(select j.town, count(*) count_post from spj t join j on j.n_izd = t.n_izd where t.n_det = ? group by j.town) a cross join(select count(*) total_post from spj t where t.n_det = ? ) b";
exec sql end declare section;

int err;
void is_error(char* st_name, int errnum) 
{
    printf("Error %d at %s\n", errnum, st_name);
    printf("%s\n", sqlca.sqlerrm.sqlerrmc);
    exec sql rollback work;
}

void Query1()
{
    printf("Query 1 \n");
    // 1. Variables
    int factor;
    exec sql begin declare section;
    int sr_ind;
    float sr;
    exec sql end declare section;

    // 3. Executing query

        exec sql begin work;
        exec sql execute stmt1 into :sr :sr_ind;
        if((err = sqlca.sqlcode) != 0 ) 
        {
            is_error("Query executing", err);
 		    EXEC SQL rollback work;
        }
        exec sql commit work;
        if (!sr_ind)
        {
            printf("sr\n");
            printf("%.2f\n", sr);
        }
        else
            printf("EMPTY TABLE\n");
}


void Query2()
{
    printf("Query 2 \n");
    // 1. Variables
    exec sql begin declare section;
    int factor;
    int sum_kol,cvet_ind;
    char n_izd[8], jname[32], n_det[6], pname[15], cvet[14],town[21];
    exec sql end declare section;

    exec sql declare curs2 cursor for stmt2;
    if((err = sqlca.sqlcode) != 0 ) 
    {
        printf("Error %d at Cursor declaring\n", err);
        printf("%s\n", sqlca.sqlerrm.sqlerrmc);
        return;
    }

    // 3. Query executing
        printf("input the town name:");
        scanf("%s", &town);
        exec sql begin work;    
        exec sql open curs2 using :town;
        if((err = sqlca.sqlcode) != 0 ) 
        {
            is_error("cursor opening", err);
            EXEC SQL rollback work;
        }
        
        exec sql fetch next from curs2 INTO :n_izd, :jname, :n_det, :pname, :cvet :cvet_ind, :sum_kol;
        if (sqlca.sqlcode == 100)
        {
            printf("Empty query!\n");
        } 
        else
        {
            if((err = sqlca.sqlcode) != 0 ) 
            {
                is_error("cursor fetching", err);
                EXEC SQL rollback work;
                exec sql close curs2;
            }
            // Checking for null
            if (cvet_ind == -1)
                sprintf(cvet, "NULL");
            printf("n_izd\t\tname\t\tn_det\tname\t\tcvet\tsum_kol\n");
            printf("%s\t%s\t%s\t%s\t%s\t%d\n", n_izd,jname,n_det,pname,cvet,sum_kol);

            while(1)
            {
                exec sql fetch next from curs2 INTO :n_izd, :jname, :n_det, :pname, :cvet :cvet_ind, :sum_kol;
                 // Checing for null
                if (sqlca.sqlcode == 100)
                    break;
                if (cvet_ind == -1)
                    sprintf(cvet, "NULL");
                if((err = sqlca.sqlcode) != 0 ) 
                {
                    exec sql close curs2;
                    is_error("cursor fetching", err);
                    break;
                }
                printf("%s\t%s\t%s\t%s\t%s\t%d\n", n_izd, jname, n_det, pname, cvet, sum_kol);
            }
            
        }
        exec sql close curs2;
        exec sql commit work;
}

void Query3()
{
    printf("Query 3 \n");
    // 1. Variables
    exec sql begin declare section;
    int count_post, total_post, num;
    float proc;
    char n_det[6],town[15];
    exec sql end declare section;

    exec sql declare curs3 cursor for stmt3;
    if((err = sqlca.sqlcode) != 0 ) 
    {
        printf("Error %d at Cursor declaring\n", err);
        printf("%s\n", sqlca.sqlerrm.sqlerrmc);
        return;
    }

    // 3. Query executing
        printf("Input n_det number:");
        scanf("%d", &num);
        sprintf(n_det, "P%d", num);
        exec sql begin work;
        exec sql open curs3 using :n_det, :n_det;
        if((err = sqlca.sqlcode) != 0 ) 
        {
            is_error("cursor opening", err);
            EXEC SQL rollback work;
        }
        exec sql fetch next from curs3 INTO :town, :count_post, :total_post, :proc;
        if (sqlca.sqlcode == 100)
        {
            printf("Empty query!\n");
        } 
        else
        {
            if((err = sqlca.sqlcode) != 0 ) 
            {
                is_error("cursor fetching", err);
                EXEC SQL rollback work;
                exec sql close curs3;
            }
            printf("town\tcount_post\ttotal_post\t\tproc\n");
            printf("%s\t%d\t\t%d\t\t%.2lf\n",town, count_post, total_post, proc);
            while(1)
            {
                exec sql fetch next from curs3 INTO :town, :count_post, :total_post, :proc;
                if (sqlca.sqlcode == 100)
                {
                    break;
                } 
                if((err = sqlca.sqlcode) != 0 ) 
                {
                    exec sql close curs3;
                    is_error("cursor fetching", err);
                    break;
                }
                printf("%s\t%d\t\t%d\t\t%.2lf\n", town, count_post, total_post, proc);
            }
        }
        // Конец транзакции
        exec sql close curs3;
        exec sql commit work; 
}

int main()
{
    int k, count = 0;
    bool query1 = true, query2 = true, query3 = true;
     // 1. DB connection
    printf("Connect to database...\n");
    EXEC SQL CONNECT TO students@fpm2 USER "pmi-b7104" using "JiHanth6";
    if((err = sqlca.sqlcode) != 0 ) 
    {
        is_error("Connection", err);
    }
    else
    {    
        printf("Connection \t[OK]\n");

        // 2. Search path
        EXEC SQL SET SEARCH_PATH TO pmib7104;
        if((err = sqlca.sqlcode) != 0 ) 
        {
            is_error("Search schema", err);
        }
        else
        {   
            
            // Query 1 preparing
            printf("preparing query 1\n");
            exec sql prepare stmt1 FROM :q1;
            if((err = sqlca.sqlcode) != 0 ) 
            {
                printf("Error %d at Query preparing\n", err);
                printf("%s\n", sqlca.sqlerrm.sqlerrmc);
                query1 = false;
                EXEC SQL rollback work; 
            }
            else printf("successful\n");
            
            // Query 2 preparing
            printf("preparing query 2\n");
            exec sql prepare stmt2 from :q2;
            if((err = sqlca.sqlcode) != 0 ) 
            {
                printf("Error %d at Query preparing\n", err);
                printf("%s\n", sqlca.sqlerrm.sqlerrmc);
                query2 = false;
                EXEC SQL rollback work; 
            }
            else printf("successful\n");
            
             // Query 3 preparing
            printf("preparing query 3\n");
            exec sql prepare stmt3 from :q3;
            if((err = sqlca.sqlcode) != 0 ) 
            {
                printf("Error %d at Query preparing\n", err);
                printf("%s\n", sqlca.sqlerrm.sqlerrmc);
                query3 = false;
                EXEC SQL rollback work;   
            }
            else printf("successful\n");

            // 3.Queryes
            do
	        {
		        printf("write a number from 1 to 3 to complete the request or 4 to exit:");
		        scanf("%d", &k);
		        switch (k)
		        {
		            case 1:
			            if (query1)
				            Query1();
			            else
				            printf("error with query1 on prepare step\n");
			            break;
		            case 2:
			            if (query2)
                            Query2();
			            else
				            printf("error with query2 on prepare step\n");
			            break;
		            case 3:
			            if (query3)
				            Query3();
			            else
				            printf("error with query3 on prepare step\n");
			            break;
		            case 4:
			            count = 1;
			            printf("session ended\n");
			            break;
		            default:
			            printf("use only number from 1 to 4!\n");
			            break;
		        }
	        } while (count == 0);
        }
    }
    // 4. Disconnect
    EXEC SQL DISCONNECT ALL;
    return 0;
}
