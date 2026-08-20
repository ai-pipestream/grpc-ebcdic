000100* Customer master record.
000200 01  CUSTOMER-RECORD.
000300     05  CUST-ID              PIC 9(6).
000400     05  CUST-NAME            PIC X(20).
000500     05  CUST-BALANCE         PIC S9(7)V99 COMP-3.
000600     05  CUST-ORDER-COUNT     PIC S9(4) COMP.
000700     05  FILLER               PIC X(4).
