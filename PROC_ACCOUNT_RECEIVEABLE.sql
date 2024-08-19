drop procedure if Exists PROC_ACCOUNT_RECEIVABLE;
DELIMITER $$
CREATE PROCEDURE `PROC_ACCOUNT_RECEIVABLE`( P_CUSTOMER_ID TEXT,
										   P_ENTRY_DATE_FROM DATETIME,
										   P_ENTRY_DATE_TO DATETIME,
										   P_COMPANY_ID INT,
										   P_START INT,
										   P_LENGTH INT )
BEGIN

	SET @QRY = CONCAT(' SELECT CASE
									WHEN E.FORM_ID IS NULL THEN NULL
									ELSE E.CUSTOMER_ID
							   END AS ''Customer'',
							   CASE
									WHEN E.FORM_ID IS NULL THEN NULL
									ELSE DATE_FORMAT(E.ENTRY_DATE, ''%m-%d-%Y'')
							   END AS ''Date of Transaction'',
							   CASE
									WHEN E.FORM_ID IS NULL THEN NULL
									ELSE DATE_FORMAT(E.DUE_DATE, ''%m-%d-%Y'')
							   END AS ''Due Date'',
							   E.FORM_ID AS ''Form ID'',
                               CASE
									WHEN E.FORM_ID IS NULL THEN NULL
									ELSE E.FORM_NAME
							   END AS ''Form'',
							   CASE
									WHEN E.FORM_ID IS NULL THEN NULL
									ELSE E.PAYPAL_TRANSACTION_ID
							   END AS ''Reference'',
							   Round(SUM(E.AMOUNT),2) AS ''Amount'',
							   Round(SUM(E.FORM_AMOUNT),2) AS ''Net Amount'',
							   Round(SUM(E.FORM_AMOUNT),2) AS ''Amount Due'',
							   CASE
									WHEN E.FORM_ID IS NULL THEN NULL
									ELSE E.TERM_NAME
							   END AS ''Term'',
							   CASE
									WHEN E.FORM_ID IS NULL THEN NULL
									ELSE E.DEFAULT_NUMBER_OF_DAYS
							   END AS ''No of Days'',
                               CASE
									WHEN E.FORM_ID IS NULL THEN NULL
									ELSE E.AGE
							   END AS ''Age'',
							   Round(SUM(E.RANGE_1),2) AS ''0 - 30'',
							   Round(SUM(E.RANGE_2),2) AS ''31 - 60'',
							   Round(SUM(E.RANGE_3),2) AS ''61 - 90'',
							   Round(SUM(E.RANGE_4),2) AS ''Over 90 Days'',
							   Round((SUM(IFNULL(E.RANGE_1, 0)) + SUM(IFNULL(E.RANGE_2, 0)) + SUM(IFNULL(E.RANGE_3, 0)) + SUM(IFNULL(E.RANGE_4, 0))),2) AS TOTAL,
                               COUNT(*) OVER() AS TOTAL_ROWS
						  FROM (
								SELECT B.CUSTOMER_ID,
										   C.INVOICE_DATE AS ENTRY_DATE,
										   DATE_ADD(C.INVOICE_DATE, INTERVAL IF(B.DEFAULT_NUMBER_OF_DAYS > 0, B.DEFAULT_NUMBER_OF_DAYS - 1, B.DEFAULT_NUMBER_OF_DAYS) DAY) AS DUE_DATE,
										   C.INVOICE_NO AS FORM_ID,
										   C.REF_NO AS ''PAYPAL_TRANSACTION_ID'',
										   C.AMOUNT AS AMOUNT,
										   (A.FORM_AMOUNT) AS FORM_AMOUNT,
										   D.TERM_NAME,
										   B.DEFAULT_NUMBER_OF_DAYS,
										   DATEDIFF(CURDATE(), C.INVOICE_DATE) AS AGE,
										   CASE 
												WHEN DATEDIFF(CURDATE(), C.INVOICE_DATE) >= 0 AND DATEDIFF(CURDATE(), C.INVOICE_DATE) <= 30 THEN (A.FORM_AMOUNT)
												ELSE NULL
										   END AS RANGE_1,
										   CASE 
												WHEN DATEDIFF(CURDATE(), C.INVOICE_DATE) >= 31 AND DATEDIFF(CURDATE(), C.INVOICE_DATE) <= 60 THEN (A.FORM_AMOUNT)
												ELSE NULL
										   END AS RANGE_2,
										   CASE 
												WHEN DATEDIFF(CURDATE(), C.INVOICE_DATE) >= 61 AND DATEDIFF(CURDATE(), C.INVOICE_DATE) <= 90 THEN (A.FORM_AMOUNT)
												ELSE NULL
										   END AS RANGE_3,
										   CASE 
												WHEN DATEDIFF(CURDATE(), C.INVOICE_DATE) >= 91 THEN (A.FORM_AMOUNT)
												ELSE NULL
										   END AS RANGE_4,
											"Beginning Balance" AS FORM_NAME
									  FROM ((((SELECT CUSTOMER_ID,
													  FORM_ID,
													  FORM_FLAG,
													  FORM_AMOUNT
												 FROM RECEIPTS_DETAIL_NEW
												WHERE IS_CONFLICTED_FULL = ''N''
												  AND FORM_AMOUNT <> 0
												  AND FORM_FLAG = ''B''
												  AND COMPANY_ID = \'',P_COMPANY_ID,'\'
												  AND CASE
														   WHEN \'',P_CUSTOMER_ID,'\' <> "" THEN CUSTOMER_ID = \'',P_CUSTOMER_ID,'\'
														   ELSE TRUE
													  END) A 
										   JOIN CUSTOMER B ON (A.CUSTOMER_ID = B.ID 
															 AND CASE
																	  WHEN \'',P_CUSTOMER_ID,'\' <> "" THEN B.ID = \'',P_CUSTOMER_ID,'\'
																	  ELSE TRUE
																 END))
										   JOIN customer_detail C ON (A.FORM_ID = C.ID 
																	AND A.FORM_FLAG = ''B''
																   AND COMPANY_ID = \'',P_COMPANY_ID,'\'
																   AND CASE
																			WHEN \'',P_CUSTOMER_ID,'\' <> "" THEN C.CUSTOMER_ID = \'',P_CUSTOMER_ID,'\'
																			ELSE TRUE
																	   END
																   AND CASE
																			WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN C.INVOICE_DATE >= \'',P_ENTRY_DATE_FROM,'\'
																			ELSE TRUE
																	   END
																   AND CASE
																			WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN C.INVOICE_DATE <= \'',P_ENTRY_DATE_TO,'\'
																			ELSE TRUE
																	   END))
										   JOIN PAYMENT_TERMS D ON (B.PAYMENT_TERM_ID = D.ID))
										   
										   UNION ALL
                          
								SELECT B.CUSTOMER_ID,
									   C.SI_ENTRY_DATE AS ENTRY_DATE,
									   DATE_ADD(C.SI_ENTRY_DATE, INTERVAL IF(B.DEFAULT_NUMBER_OF_DAYS > 0, B.DEFAULT_NUMBER_OF_DAYS - 1, B.DEFAULT_NUMBER_OF_DAYS) DAY) AS DUE_DATE,
									   C.SI_ID AS FORM_ID,
									   C.PAYPAL_TRANSACTION_ID,
									   C.SI_TOTAL AS AMOUNT,
									   (A.FORM_AMOUNT) AS FORM_AMOUNT,
									   D.TERM_NAME,
									   B.DEFAULT_NUMBER_OF_DAYS,
									   DATEDIFF(CURDATE(), C.SI_ENTRY_DATE) AS AGE,
									   CASE 
											WHEN DATEDIFF(CURDATE(), C.SI_ENTRY_DATE) >= 0 AND DATEDIFF(CURDATE(), C.SI_ENTRY_DATE) <= 30 THEN (A.FORM_AMOUNT)
											ELSE NULL
									   END AS RANGE_1,
									   CASE 
											WHEN DATEDIFF(CURDATE(), C.SI_ENTRY_DATE) >= 31 AND DATEDIFF(CURDATE(), C.SI_ENTRY_DATE) <= 60 THEN (A.FORM_AMOUNT)
											ELSE NULL
									   END AS RANGE_2,
									   CASE 
											WHEN DATEDIFF(CURDATE(), C.SI_ENTRY_DATE) >= 61 AND DATEDIFF(CURDATE(), C.SI_ENTRY_DATE) <= 90 THEN (A.FORM_AMOUNT)
											ELSE NULL
									   END AS RANGE_3,
									   CASE 
											WHEN DATEDIFF(CURDATE(), C.SI_ENTRY_DATE) >= 91 THEN (A.FORM_AMOUNT)
											ELSE NULL
									   END AS RANGE_4,
                                       ''Sale Invoice'' AS FORM_NAME
								  FROM ((((SELECT CUSTOMER_ID,
												  FORM_ID,
                                                  FORM_FLAG,
                                                  FORM_AMOUNT
											 FROM RECEIPTS_DETAIL_NEW
											WHERE IS_CONFLICTED_FULL = ''N''
                                              AND FORM_AMOUNT <> 0
                                              AND FORM_FLAG = ''I''
                                              AND COMPANY_ID = \'',P_COMPANY_ID,'\'
                                              AND CASE
													   WHEN \'',P_CUSTOMER_ID,'\' <> "" THEN CUSTOMER_ID = \'',P_CUSTOMER_ID,'\'
													   ELSE TRUE
												  END) A 
									   JOIN CUSTOMER B ON (A.CUSTOMER_ID = B.ID 
														 AND CASE
																  WHEN \'',P_CUSTOMER_ID,'\' <> "" THEN B.ID = \'',P_CUSTOMER_ID,'\'
																  ELSE TRUE
															 END))
									   JOIN SALE_INVOICE C ON (A.FORM_ID = C.ID 
															   AND A.FORM_FLAG = ''I''
															   AND C.COMPANY_ID = \'',P_COMPANY_ID,'\'
															   AND CASE
																		WHEN \'',P_CUSTOMER_ID,'\' <> "" THEN C.CUSTOMER_ID = \'',P_CUSTOMER_ID,'\'
																		ELSE TRUE
																   END
															   AND CASE
																		WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN C.SI_ENTRY_DATE >= \'',P_ENTRY_DATE_FROM,'\'
																		ELSE TRUE
																   END
															   AND CASE
																		WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN C.SI_ENTRY_DATE <= \'',P_ENTRY_DATE_TO,'\'
																		ELSE TRUE
																   END))
									   JOIN PAYMENT_TERMS D ON (B.PAYMENT_TERM_ID = D.ID))
                                       
                                       UNION ALL
                                       
								SELECT B.CUSTOMER_ID,
									   C.ST_ENTRY_DATE AS ENTRY_DATE,
									   DATE_ADD(C.ST_ENTRY_DATE, INTERVAL B.DEFAULT_NUMBER_OF_DAYS DAY) AS DUE_DATE,
									   C.ST_ID AS FORM_ID,
									   C.ST_ID AS PAYPAL_TRANSACTION_ID,
									   C.ST_TOTAL AS AMOUNT,
									   (A.FORM_AMOUNT) AS FORM_AMOUNT,
									   D.TERM_NAME,
									   B.DEFAULT_NUMBER_OF_DAYS,
									   DATEDIFF(CURDATE(), C.ST_ENTRY_DATE) AS AGE,
									   CASE 
											WHEN DATEDIFF(CURDATE(), C.ST_ENTRY_DATE) >= 0 AND DATEDIFF(CURDATE(), C.ST_ENTRY_DATE) <= 30 THEN (A.FORM_AMOUNT)
											ELSE NULL
									   END AS RANGE_1,
									   CASE 
											WHEN DATEDIFF(CURDATE(), C.ST_ENTRY_DATE) >= 31 AND DATEDIFF(CURDATE(), C.ST_ENTRY_DATE) <= 60 THEN (A.FORM_AMOUNT)
											ELSE NULL
									   END AS RANGE_2,
									   CASE 
											WHEN DATEDIFF(CURDATE(), C.ST_ENTRY_DATE) >= 61 AND DATEDIFF(CURDATE(), C.ST_ENTRY_DATE) <= 90 THEN (A.FORM_AMOUNT)
											ELSE NULL
									   END AS RANGE_3,
									   CASE 
											WHEN DATEDIFF(CURDATE(), C.ST_ENTRY_DATE) >= 91 THEN (A.FORM_AMOUNT)
											ELSE NULL
									   END AS RANGE_4,
                                       ''Stock Transfer'' AS FORM_NAME
								  FROM ((((SELECT CUSTOMER_ID,
												  FORM_ID,
                                                  FORM_FLAG,
                                                  FORM_AMOUNT
											 FROM RECEIPTS_DETAIL_NEW
											WHERE IS_CONFLICTED_FULL = ''N''
                                              AND FORM_AMOUNT <> 0
                                              AND FORM_FLAG = ''T''
                                              AND COMPANY_ID = \'',P_COMPANY_ID,'\'
                                              AND CASE
													   WHEN \'',P_CUSTOMER_ID,'\' <> "" THEN CUSTOMER_ID = \'',P_CUSTOMER_ID,'\'
													   ELSE TRUE
												  END) A 
									   JOIN CUSTOMER B ON (A.CUSTOMER_ID = B.ID 
														 AND CASE
																  WHEN \'',P_CUSTOMER_ID,'\' <> "" THEN B.ID = \'',P_CUSTOMER_ID,'\'
																  ELSE TRUE
															 END))
									   JOIN VW_STOCK_TRANSFER C ON (A.FORM_ID = C.ID 
															   AND A.FORM_FLAG = ''T''
															   AND C.COMPANY_FROM_ID = \'',P_COMPANY_ID,'\'
															   AND CASE
																		WHEN \'',P_CUSTOMER_ID,'\' <> "" THEN C.CUSTOMER_ID = \'',P_CUSTOMER_ID,'\'
																		ELSE TRUE
																   END
															   AND CASE
																		WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN C.ST_ENTRY_DATE >= \'',P_ENTRY_DATE_FROM,'\'
																		ELSE TRUE
																   END
															   AND CASE
																		WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN C.ST_ENTRY_DATE <= \'',P_ENTRY_DATE_TO,'\'
																		ELSE TRUE
																   END))
									   JOIN PAYMENT_TERMS D ON (B.PAYMENT_TERM_ID = D.ID))
                                       
                                       UNION ALL
                                       
								SELECT B.CUSTOMER_ID,
									   C.REP_ENTRY_DATE AS ENTRY_DATE,
									   DATE_ADD(C.REP_ENTRY_DATE, INTERVAL B.DEFAULT_NUMBER_OF_DAYS DAY) AS DUE_DATE,
									   C.REP_ID AS FORM_ID,
									   C.PAYPAL_TRANSACTION_ID,
									   C.BALANCE AS AMOUNT,
									   (A.FORM_AMOUNT) AS FORM_AMOUNT,
									   D.TERM_NAME,
									   B.DEFAULT_NUMBER_OF_DAYS,
									   DATEDIFF(CURDATE(), C.REP_ENTRY_DATE) AS AGE,
									   CASE 
											WHEN DATEDIFF(CURDATE(), C.REP_ENTRY_DATE) >= 0 AND DATEDIFF(CURDATE(), C.REP_ENTRY_DATE) <= 30 THEN (A.FORM_AMOUNT)
											ELSE NULL
									   END AS RANGE_1,
									   CASE 
											WHEN DATEDIFF(CURDATE(), C.REP_ENTRY_DATE) >= 31 AND DATEDIFF(CURDATE(), C.REP_ENTRY_DATE) <= 60 THEN (A.FORM_AMOUNT)
											ELSE NULL
									   END AS RANGE_2,
									   CASE 
											WHEN DATEDIFF(CURDATE(), C.REP_ENTRY_DATE) >= 61 AND DATEDIFF(CURDATE(), C.REP_ENTRY_DATE) <= 90 THEN (A.FORM_AMOUNT)
											ELSE NULL
									   END AS RANGE_3,
									   CASE 
											WHEN DATEDIFF(CURDATE(), C.REP_ENTRY_DATE) >= 91 THEN (A.FORM_AMOUNT)
											ELSE NULL
									   END AS RANGE_4,
                                       ''Replacement'' AS FORM_NAME
								  FROM ((((SELECT CUSTOMER_ID,
												  FORM_ID,
                                                  FORM_FLAG,
                                                  FORM_AMOUNT
											 FROM RECEIPTS_DETAIL_NEW
											WHERE IS_CONFLICTED_FULL = ''N''
                                              AND FORM_AMOUNT <> 0
                                              AND FORM_FLAG = ''E''
                                              AND COMPANY_ID = \'',P_COMPANY_ID,'\'
                                              AND CASE
													   WHEN \'',P_CUSTOMER_ID,'\' <> "" THEN CUSTOMER_ID = \'',P_CUSTOMER_ID,'\'
													   ELSE TRUE
												  END) A 
									   JOIN CUSTOMER B ON (A.CUSTOMER_ID = B.ID 
														 AND CASE
																  WHEN \'',P_CUSTOMER_ID,'\' <> "" THEN B.ID = \'',P_CUSTOMER_ID,'\'
																  ELSE TRUE
															 END))
									   JOIN REPLACEMENT C ON (A.FORM_ID = C.ID 
															   AND A.FORM_FLAG = ''E''
															   AND C.COMPANY_ID = \'',P_COMPANY_ID,'\'
															   AND CASE
																		WHEN \'',P_CUSTOMER_ID,'\' <> "" THEN C.CUSTOMER_ID = \'',P_CUSTOMER_ID,'\'
																		ELSE TRUE
																   END
															   AND CASE
																		WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN C.REP_ENTRY_DATE >= \'',P_ENTRY_DATE_FROM,'\'
																		ELSE TRUE
																   END
															   AND CASE
																		WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN C.REP_ENTRY_DATE <= \'',P_ENTRY_DATE_TO,'\'
																		ELSE TRUE
																   END))
									   JOIN PAYMENT_TERMS D ON (B.PAYMENT_TERM_ID = D.ID))
                                       
                                       UNION ALL
                                       
								SELECT B.CUSTOMER_ID,
									   C.SR_ENTRY_DATE AS ENTRY_DATE,
									   DATE_ADD(C.SR_ENTRY_DATE, INTERVAL B.DEFAULT_NUMBER_OF_DAYS DAY) AS DUE_DATE,
									   C.SR_ID AS FORM_ID,
									   C.PAYPAL_TRANSACTION_ID,
									   C.SR_TOTAL AS AMOUNT,
									   (A.FORM_AMOUNT) AS FORM_AMOUNT,
									   D.TERM_NAME,
									   B.DEFAULT_NUMBER_OF_DAYS,
									   DATEDIFF(CURDATE(), C.SR_ENTRY_DATE) AS AGE,
									   CASE 
											WHEN DATEDIFF(CURDATE(), C.SR_ENTRY_DATE) >= 0 AND DATEDIFF(CURDATE(), C.SR_ENTRY_DATE) <= 30 THEN (A.FORM_AMOUNT)
											ELSE NULL
									   END AS RANGE_1,
									   CASE 
											WHEN DATEDIFF(CURDATE(), C.SR_ENTRY_DATE) >= 31 AND DATEDIFF(CURDATE(), C.SR_ENTRY_DATE) <= 60 THEN (A.FORM_AMOUNT)
											ELSE NULL
									   END AS RANGE_2,
									   CASE 
											WHEN DATEDIFF(CURDATE(), C.SR_ENTRY_DATE) >= 61 AND DATEDIFF(CURDATE(), C.SR_ENTRY_DATE) <= 90 THEN (A.FORM_AMOUNT)
											ELSE NULL
									   END AS RANGE_3,
									   CASE 
											WHEN DATEDIFF(CURDATE(), C.SR_ENTRY_DATE) >= 91 THEN (A.FORM_AMOUNT)
											ELSE NULL
									   END AS RANGE_4,
                                       ''Sale Return'' AS FORM_NAME
								  FROM ((((SELECT CUSTOMER_ID,
												  FORM_ID,
                                                  FORM_FLAG,
                                                  FORM_AMOUNT
											 FROM RECEIPTS_DETAIL_NEW
											WHERE IS_CONFLICTED_FULL = ''N''
                                              AND FORM_AMOUNT <> 0
                                              AND FORM_FLAG = ''S''
                                              AND COMPANY_ID = \'',P_COMPANY_ID,'\'
                                              AND CASE
													   WHEN \'',P_CUSTOMER_ID,'\' <> "" THEN CUSTOMER_ID = \'',P_CUSTOMER_ID,'\'
													   ELSE TRUE
												  END) A 
									   JOIN CUSTOMER B ON (A.CUSTOMER_ID = B.ID 
														 AND CASE
																  WHEN \'',P_CUSTOMER_ID,'\' <> "" THEN B.ID = \'',P_CUSTOMER_ID,'\'
																  ELSE TRUE
															 END))
									   JOIN SALE_RETURN C ON (A.FORM_ID = C.ID 
															   AND A.FORM_FLAG = ''S''
															   AND C.COMPANY_ID = \'',P_COMPANY_ID,'\'
															   AND CASE
																		WHEN \'',P_CUSTOMER_ID,'\' <> "" THEN C.CUSTOMER_ID = \'',P_CUSTOMER_ID,'\'
																		ELSE TRUE
																   END
															   AND CASE
																		WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN C.SR_ENTRY_DATE >= \'',P_ENTRY_DATE_FROM,'\'
																		ELSE TRUE
																   END
															   AND CASE
																		WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN C.SR_ENTRY_DATE <= \'',P_ENTRY_DATE_TO,'\'
																		ELSE TRUE
																   END))
									   JOIN PAYMENT_TERMS D ON (B.PAYMENT_TERM_ID = D.ID))
                                       
                                       UNION ALL
                                       
								SELECT B.CUSTOMER_ID,
									   C.PC_ENTRY_DATE AS ENTRY_DATE,
									   DATE_ADD(C.PC_ENTRY_DATE, INTERVAL B.DEFAULT_NUMBER_OF_DAYS DAY) AS DUE_DATE,
									   C.PC_ID AS FORM_ID,
									   C.PAYPAL_TRANSACTION_ID,
									   C.TOTAL_AMOUNT AS AMOUNT,
									   (A.FORM_AMOUNT) AS FORM_AMOUNT,
									   D.TERM_NAME,
									   B.DEFAULT_NUMBER_OF_DAYS,
									   DATEDIFF(CURDATE(), C.PC_ENTRY_DATE) AS AGE,
									   CASE 
											WHEN DATEDIFF(CURDATE(), C.PC_ENTRY_DATE) >= 0 AND DATEDIFF(CURDATE(), C.PC_ENTRY_DATE) <= 30 THEN (A.FORM_AMOUNT)
											ELSE NULL
									   END AS RANGE_1,
									   CASE 
											WHEN DATEDIFF(CURDATE(), C.PC_ENTRY_DATE) >= 31 AND DATEDIFF(CURDATE(), C.PC_ENTRY_DATE) <= 60 THEN (A.FORM_AMOUNT)
											ELSE NULL
									   END AS RANGE_2,
									   CASE 
											WHEN DATEDIFF(CURDATE(), C.PC_ENTRY_DATE) >= 61 AND DATEDIFF(CURDATE(), C.PC_ENTRY_DATE) <= 90 THEN (A.FORM_AMOUNT)
											ELSE NULL
									   END AS RANGE_3,
									   CASE 
											WHEN DATEDIFF(CURDATE(), C.PC_ENTRY_DATE) >= 91 THEN (A.FORM_AMOUNT)
											ELSE NULL
									   END AS RANGE_4,
                                       ''Partial Credit'' AS FORM_NAME
								  FROM ((((SELECT CUSTOMER_ID,
												  FORM_ID,
                                                  FORM_FLAG,
                                                  FORM_AMOUNT
											 FROM RECEIPTS_DETAIL_NEW
											WHERE IS_CONFLICTED_FULL = ''N''
                                              AND FORM_AMOUNT <> 0
                                              AND FORM_FLAG = ''L''
                                              AND COMPANY_ID = \'',P_COMPANY_ID,'\'
                                              AND CASE
													   WHEN \'',P_CUSTOMER_ID,'\' <> "" THEN CUSTOMER_ID = \'',P_CUSTOMER_ID,'\'
													   ELSE TRUE
												  END) A 
									   JOIN CUSTOMER B ON (A.CUSTOMER_ID = B.ID 
														 AND CASE
																  WHEN \'',P_CUSTOMER_ID,'\' <> "" THEN B.ID = \'',P_CUSTOMER_ID,'\'
																  ELSE TRUE
															 END))
									   JOIN PARTIAL_CREDIT C ON (A.FORM_ID = C.ID 
															   AND A.FORM_FLAG = ''L''
															   AND C.COMPANY_ID = \'',P_COMPANY_ID,'\'
															   AND CASE
																		WHEN \'',P_CUSTOMER_ID,'\' <> "" THEN C.CUSTOMER_ID = \'',P_CUSTOMER_ID,'\'
																		ELSE TRUE
																   END
															   AND CASE
																		WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN C.PC_ENTRY_DATE >= \'',P_ENTRY_DATE_FROM,'\'
																		ELSE TRUE
																   END
															   AND CASE
																		WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN C.PC_ENTRY_DATE <= \'',P_ENTRY_DATE_TO,'\'
																		ELSE TRUE
																   END))
									   JOIN PAYMENT_TERMS D ON (B.PAYMENT_TERM_ID = D.ID))
                                       
                                       UNION ALL
                                       
								SELECT B.CUSTOMER_ID,
									   C.PS_ENTRY_DATE AS ENTRY_DATE,
									   DATE_ADD(C.PS_ENTRY_DATE, INTERVAL B.DEFAULT_NUMBER_OF_DAYS DAY) AS DUE_DATE,
									   C.PAYMENT_SENT_ID AS FORM_ID,
									   C.PAYPAL_TRANSACTION_ID,
									   C.AMOUNT AS AMOUNT,
									   (A.FORM_AMOUNT) AS FORM_AMOUNT,
									   D.TERM_NAME,
									   B.DEFAULT_NUMBER_OF_DAYS,
									   DATEDIFF(CURDATE(), C.PS_ENTRY_DATE) AS AGE,
									   CASE 
											WHEN DATEDIFF(CURDATE(), C.PS_ENTRY_DATE) >= 0 AND DATEDIFF(CURDATE(), C.PS_ENTRY_DATE) <= 30 THEN (A.FORM_AMOUNT)
											ELSE NULL
									   END AS RANGE_1,
									   CASE 
											WHEN DATEDIFF(CURDATE(), C.PS_ENTRY_DATE) >= 31 AND DATEDIFF(CURDATE(), C.PS_ENTRY_DATE) <= 60 THEN (A.FORM_AMOUNT)
											ELSE NULL
									   END AS RANGE_2,
									   CASE 
											WHEN DATEDIFF(CURDATE(), C.PS_ENTRY_DATE) >= 61 AND DATEDIFF(CURDATE(), C.PS_ENTRY_DATE) <= 90 THEN (A.FORM_AMOUNT)
											ELSE NULL
									   END AS RANGE_3,
									   CASE 
											WHEN DATEDIFF(CURDATE(), C.PS_ENTRY_DATE) >= 91 THEN (A.FORM_AMOUNT)
											ELSE NULL
									   END AS RANGE_4,
                                       ''Payment Sent'' AS FORM_NAME
								  FROM ((((SELECT CUSTOMER_ID,
												  FORM_ID,
                                                  FORM_FLAG,
                                                  FORM_AMOUNT
											 FROM RECEIPTS_DETAIL_NEW
											WHERE IS_CONFLICTED_FULL = ''N''
                                              AND FORM_AMOUNT <> 0
                                              AND FORM_FLAG = ''P''
                                              AND COMPANY_ID = \'',P_COMPANY_ID,'\'
                                              AND CASE
													   WHEN \'',P_CUSTOMER_ID,'\' <> "" THEN CUSTOMER_ID = \'',P_CUSTOMER_ID,'\'
													   ELSE TRUE
												  END) A 
									   JOIN CUSTOMER B ON (A.CUSTOMER_ID = B.ID 
														 AND CASE
																  WHEN \'',P_CUSTOMER_ID,'\' <> "" THEN B.ID = \'',P_CUSTOMER_ID,'\'
																  ELSE TRUE
															 END))
									   JOIN PAYMENT_SENT C ON (A.FORM_ID = C.ID 
															   AND A.FORM_FLAG = ''P''
															   AND C.COMPANY_ID = \'',P_COMPANY_ID,'\'
															   AND CASE
																		WHEN \'',P_CUSTOMER_ID,'\' <> "" THEN C.CUSTOMER_ID = \'',P_CUSTOMER_ID,'\'
																		ELSE TRUE
																   END
															   AND CASE
																		WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN C.PS_ENTRY_DATE >= \'',P_ENTRY_DATE_FROM,'\'
																		ELSE TRUE
																   END
															   AND CASE
																		WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN C.PS_ENTRY_DATE <= \'',P_ENTRY_DATE_TO,'\'
																		ELSE TRUE
																   END))
									   JOIN PAYMENT_TERMS D ON (B.PAYMENT_TERM_ID = D.ID))
                                       
                                       UNION ALL
                                       
								SELECT B.CUSTOMER_ID,
									   C.RM_ENTRY_DATE AS ENTRY_DATE,
									   DATE_ADD(C.RM_ENTRY_DATE, INTERVAL B.DEFAULT_NUMBER_OF_DAYS DAY) AS DUE_DATE,
									   C.RECEIVE_MONEY_ID AS FORM_ID,
									   C.PAYPAL_TRANSACTION_ID,
									   C.AMOUNT AS AMOUNT,
									   (A.FORM_AMOUNT) AS FORM_AMOUNT,
									   D.TERM_NAME,
									   B.DEFAULT_NUMBER_OF_DAYS,
									   DATEDIFF(CURDATE(), C.RM_ENTRY_DATE) AS AGE,
									   CASE 
											WHEN DATEDIFF(CURDATE(), C.RM_ENTRY_DATE) >= 0 AND DATEDIFF(CURDATE(), C.RM_ENTRY_DATE) <= 30 THEN (A.FORM_AMOUNT)
											ELSE NULL
									   END AS RANGE_1,
									   CASE 
											WHEN DATEDIFF(CURDATE(), C.RM_ENTRY_DATE) >= 31 AND DATEDIFF(CURDATE(), C.RM_ENTRY_DATE) <= 60 THEN (A.FORM_AMOUNT)
											ELSE NULL
									   END AS RANGE_2,
									   CASE 
											WHEN DATEDIFF(CURDATE(), C.RM_ENTRY_DATE) >= 61 AND DATEDIFF(CURDATE(), C.RM_ENTRY_DATE) <= 90 THEN (A.FORM_AMOUNT)
											ELSE NULL
									   END AS RANGE_3,
									   CASE 
											WHEN DATEDIFF(CURDATE(), C.RM_ENTRY_DATE) >= 91 THEN (A.FORM_AMOUNT)
											ELSE NULL
									   END AS RANGE_4,
                                       ''Receive Money'' AS FORM_NAME
								  FROM ((((SELECT CUSTOMER_ID,
												  FORM_ID,
                                                  FORM_FLAG,
                                                  FORM_AMOUNT
											 FROM RECEIPTS_DETAIL_NEW
											WHERE IS_CONFLICTED_FULL = ''N''
                                              AND FORM_AMOUNT <> 0
                                              AND FORM_FLAG = ''M''
                                              AND COMPANY_ID = \'',P_COMPANY_ID,'\'
                                              AND CASE
													   WHEN \'',P_CUSTOMER_ID,'\' <> "" THEN CUSTOMER_ID = \'',P_CUSTOMER_ID,'\'
													   ELSE TRUE
												  END) A 
									   JOIN CUSTOMER B ON (A.CUSTOMER_ID = B.ID 
														 AND CASE
																  WHEN \'',P_CUSTOMER_ID,'\' <> "" THEN B.ID = \'',P_CUSTOMER_ID,'\'
																  ELSE TRUE
															 END))
									   JOIN RECEIVE_MONEY C ON (A.FORM_ID = C.ID 
															   AND A.FORM_FLAG = ''M''
															   AND C.COMPANY_ID = \'',P_COMPANY_ID,'\'
															   AND CASE
																		WHEN \'',P_CUSTOMER_ID,'\' <> "" THEN C.CUSTOMER_ID = \'',P_CUSTOMER_ID,'\'
																		ELSE TRUE
																   END
															   AND CASE
																		WHEN \'',P_ENTRY_DATE_FROM,'\' <> "" THEN C.RM_ENTRY_DATE >= \'',P_ENTRY_DATE_FROM,'\'
																		ELSE TRUE
																   END
															   AND CASE
																		WHEN \'',P_ENTRY_DATE_TO,'\' <> "" THEN C.RM_ENTRY_DATE <= \'',P_ENTRY_DATE_TO,'\'
																		ELSE TRUE
																   END))
									   JOIN PAYMENT_TERMS D ON (B.PAYMENT_TERM_ID = D.ID))
                               ) E
                      GROUP BY E.CUSTOMER_ID, E.FORM_ID, E.FORM_NAME, E.ENTRY_DATE, E.DUE_DATE, E.PAYPAL_TRANSACTION_ID, E.TERM_NAME, E.AGE, E.DEFAULT_NUMBER_OF_DAYS WITH ROLLUP
					    HAVING (E.DEFAULT_NUMBER_OF_DAYS IS NOT NULL) OR E.FORM_ID IS NULL
						 LIMIT ',P_START,', ',P_LENGTH,';');
                         
    PREPARE STMP FROM @QRY;
    EXECUTE STMP ;
    DEALLOCATE PREPARE STMP;
    
END $$
DELIMITER ;