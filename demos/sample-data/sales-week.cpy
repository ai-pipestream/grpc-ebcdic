000100* Weekly sales summary, one row per store per week.
000200 01  SALES-RECORD.
000300     05  STORE-ID             PIC X(4).
000400     05  WEEK-NUM            PIC 9(2).
000500     05  DAILY-TOTAL         PIC S9(5)V99 COMP-3 OCCURS 7.
