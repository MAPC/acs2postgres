﻿select g.geoid, g.logrecno, g.name, 
--OWN_OCC_ME-----------------------------------------------------------------------------
e._001 as OWN_OCC, m._001 as OWN_OCC_ME,

--OWNOCCV2-------------------------------------------------------------------------------
e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022 AS OWNOCCV2,

--OWNOCCV2_ME----------------------------------------------------------------------------
sqrt(power(m._003, 2)+ power(m._004, 2)+ power(m._005, 2)+ power(m._006, 2)+
power(m._007, 2)+ power(m._008, 2)+ power(m._009, 2)+ power(m._010, 2)+
power(m._011, 2)+ power(m._014, 2)+ power(m._015, 2)+ power(m._016, 2)+
power(m._017, 2)+ power(m._018, 2)+ power(m._019, 2)+ power(m._020, 2)+
power(m._021, 2)+ power(m._022, 2)) AS OWNOCCV2_ME,

--NOT_CB---------------------------------------------------------------------------------
e._003 + e._004 + e._005 + e._006 + e._007 +
e._014 + e._015 + e._016 + e._017 + e._018 as NOT_CB,

--NOT_CB_ME------------------------------------------------------------------------------
sqrt(power(m._003, 2)+ power(m._004, 2)+ power(m._005, 2)+ power(m._006, 2)+
power(m._007, 2)+ power(m._014, 2)+ power(m._015, 2)+ power(m._016, 2)+
power(m._017, 2)+ power(m._018, 2)) as NOT_CB_ME,

--CB-------------------------------------------------------------------------------------
e._008 + e._009 + e._010 + e._011 + e._019 +
e._020 + e._021 + e._022 as CB,

--CB_ME----------------------------------------------------------------------------------
sqrt(power(m._008, 2) + power(m._009, 2) + power(m._010, 2) +
power(m._011, 2) + power(m._019, 2) + power(m._020, 2) +
power(m._021, 2) + power(m._022, 2)) as CB_ME,

--CB_3050--------------------------------------------------------------------------------
e._008 + e._009 + e._010 + e._019 + e._020 + e._021 as CB_3050,

--CB_CB_3050_ME--------------------------------------------------------------------------
sqrt(power(m._008, 2) + power(m._009, 2) + power(m._010, 2) +
power(m._019, 2) + power(m._020, 2) + power(m._021, 2)) as CB_3050_ME,

--CB_50----------------------------------------------------------------------------------
e._011 + e._022 as CB_50,

--CB_50_ME-------------------------------------------------------------------------------
sqrt(power(m._011, 2) + power(m._022, 2)) as CB_50_ME,

--NOT_CB_P-------------------------------------------------------------------------------
CASE 
WHEN (e._003 + e._004 + e._005 + e._006 + e._007 +
 e._014 + e._015 + e._016 + e._017 + e._018)=0 then 0.0

WHEN (e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022)=0 then 0.0

 WHEN (e._003 + e._004 + e._005 + e._006 + e._007 +
 e._014 + e._015 + e._016 + e._017 + e._018) is NULL then -.99999

WHEN (e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022) is NULL then -.99999

ELSE
cast(e._003 + e._004 + 
e._005 + e._006 + e._007 +
e._014 + e._015 + e._016 + e._017 + e._018 as FLOAT)*100 /
cast(e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022 as FLOAT)
END as NOT_CB_P,

--NOT_CB_ME_P----------------------------------------------------------------------------
CASE
WHEN (e._003 + e._004 + e._005 + e._006 + e._007 +
 e._014 + e._015 + e._016 + e._017 + e._018) is NULL then -.99999

WHEN (e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022) is NULL then -.99999

when (e._003 + e._004 + e._005 + e._006 + e._007 +
 e._014 + e._015 + e._016 + e._017 + e._018)=0 then 0

 when (e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022) = 0 then 0

when /* (1/OWNOCCV2) * SQRT(NOT_CBME^2-(NOT_CB^2/OWNOCCV2^2) * OWNOCCV2ME^2)*100 */
(power(m._003, 2)+ power(m._004, 2)+ power(m._005, 2)+ power(m._006, 2)+ --NOT_CBME^2
power(m._007, 2)+ power(m._014, 2)+ power(m._015, 2)+ power(m._016, 2)+
power(m._017, 2)+ power(m._018, 2)) 
> 
(
power(e._003 + e._004 + e._005 + e._006 + e._007 + --NOT_CB^2
e._014 + e._015 + e._016 + e._017 + e._018, 2) 
/ 
power(e._003 + e._004 + e._005 + e._006 + e._007 + --OWNOCCV2^2
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022, 2)
)*
(power(m._003, 2)+ power(m._004, 2)+ power(m._005, 2)+ power(m._006, 2)+ --OWNOCCV2ME^2
power(m._007, 2)+ power(m._008, 2)+ power(m._009, 2)+ power(m._010, 2)+
power(m._011, 2)+ power(m._014, 2)+ power(m._015, 2)+ power(m._016, 2)+
power(m._017, 2)+ power(m._018, 2)+ power(m._019, 2)+ power(m._020, 2)+
power(m._021, 2)+ power(m._022, 2)) 
then
(100.0/ (e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022)) *

SQRT( (power(m._003, 2)+ power(m._004, 2)+ power(m._005, 2)+ power(m._006, 2)+
power(m._007, 2)+ power(m._014, 2)+ power(m._015, 2)+ power(m._016, 2)+
power(m._017, 2)+ power(m._018, 2)) - 

(power(e._003 + e._004 + e._005 + e._006 + e._007 +
e._014 + e._015 + e._016 + e._017 + e._018, 2) 
/ 
power(e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022, 2)) *

(power(m._003, 2)+ power(m._004, 2)+ power(m._005, 2)+ power(m._006, 2)+ --
power(m._007, 2)+ power(m._008, 2)+ power(m._009, 2)+ power(m._010, 2)+
power(m._011, 2)+ power(m._014, 2)+ power(m._015, 2)+ power(m._016, 2)+
power(m._017, 2)+ power(m._018, 2)+ power(m._019, 2)+ power(m._020, 2)+
power(m._021, 2)+ power(m._022, 2)))

else /* (1/OWNOCCV2) * SQRT(NOT_CBME^2+(NOT_CB^2/OWNOCCV2^2) * OWNOCCV2ME^2)*100 */
(100.0/ (e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022)) *

SQRT( (power(m._003, 2)+ power(m._004, 2)+ power(m._005, 2)+ power(m._006, 2)+
power(m._007, 2)+ power(m._014, 2)+ power(m._015, 2)+ power(m._016, 2)+
power(m._017, 2)+ power(m._018, 2)) + 

(power(e._003 + e._004 + e._005 + e._006 + e._007 +
e._014 + e._015 + e._016 + e._017 + e._018, 2) 
/ 
power(e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022, 2)) *

(power(m._003, 2)+ power(m._004, 2)+ power(m._005, 2)+ power(m._006, 2)+ --
power(m._007, 2)+ power(m._008, 2)+ power(m._009, 2)+ power(m._010, 2)+
power(m._011, 2)+ power(m._014, 2)+ power(m._015, 2)+ power(m._016, 2)+
power(m._017, 2)+ power(m._018, 2)+ power(m._019, 2)+ power(m._020, 2)+
power(m._021, 2)+ power(m._022, 2)))

END as NOT_CB_ME_P,

--CB_P-----------------------------------------------------------------------------------
CASE 
WHEN (e._008 + e._009 + e._010 + e._011 + e._019 +
e._020 + e._021 + e._022)=0 then 0.0

WHEN (e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022)=0 then 0.0

WHEN (e._008 + e._009 + e._010 + e._011 + e._019 +
e._020 + e._021 + e._022) is NULL then -.99999

WHEN (e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022) is NULL then -.99999

ELSE
cast(e._008 + e._009 + e._010 + e._011 + e._019 +
e._020 + e._021 + e._022 as FLOAT)*100 /
cast(e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022 as FLOAT)
END as CB_P,

--CB_ME_P--------------------------------------------------------------------------------
CASE
WHEN (e._008 + e._009 + e._010 + e._011 + e._019 +
e._020 + e._021 + e._022) is NULL then -.99999

WHEN (e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022) is NULL then -.99999

when (e._008 + e._009 + e._010 + e._011 + e._019 +
e._020 + e._021 + e._022)=0 then 0

 when (e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022) = 0 then 0

when /* (1/OWNOCCV2) * SQRT(CB_ME^2-(CB^2/OWNOCCV2^2) * OWNOCCV2ME^2)*100 */
(power(m._008, 2) + power(m._009, 2) + power(m._010, 2) +
power(m._011, 2) + power(m._019, 2) + power(m._020, 2) +
power(m._021, 2) + power(m._022, 2)) 
> 
(
power(e._008 + e._009 + e._010 + e._011 + e._019 +
e._020 + e._021 + e._022, 2) 
/ 
power(e._003 + e._004 + e._005 + e._006 + e._007 + --OWNOCCV2^2
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022, 2)
)*
(power(m._003, 2)+ power(m._004, 2)+ power(m._005, 2)+ power(m._006, 2)+ --OWNOCCV2ME^2
power(m._007, 2)+ power(m._008, 2)+ power(m._009, 2)+ power(m._010, 2)+
power(m._011, 2)+ power(m._014, 2)+ power(m._015, 2)+ power(m._016, 2)+
power(m._017, 2)+ power(m._018, 2)+ power(m._019, 2)+ power(m._020, 2)+
power(m._021, 2)+ power(m._022, 2)) 
then
(100.0/ (e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022)) *

SQRT( (power(m._008, 2) + power(m._009, 2) + power(m._010, 2) +
power(m._011, 2) + power(m._019, 2) + power(m._020, 2) +
power(m._021, 2) + power(m._022, 2)) - 

(power(e._008 + e._009 + e._010 + e._011 + e._019 +
e._020 + e._021 + e._022, 2) 
/ 
power(e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022, 2)) *

(power(m._003, 2)+ power(m._004, 2)+ power(m._005, 2)+ power(m._006, 2)+ --
power(m._007, 2)+ power(m._008, 2)+ power(m._009, 2)+ power(m._010, 2)+
power(m._011, 2)+ power(m._014, 2)+ power(m._015, 2)+ power(m._016, 2)+
power(m._017, 2)+ power(m._018, 2)+ power(m._019, 2)+ power(m._020, 2)+
power(m._021, 2)+ power(m._022, 2)))

else /* (1/OWNOCCV2) * SQRT(CB_ME^2+(CB^2/OWNOCCV2^2) * OWNOCCV2ME^2)*100 */
(100.0/ (e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022)) *

SQRT( (power(m._008, 2) + power(m._009, 2) + power(m._010, 2) +
power(m._011, 2) + power(m._019, 2) + power(m._020, 2) +
power(m._021, 2) + power(m._022, 2)) + 

(power(e._008 + e._009 + e._010 + e._011 + e._019 +
e._020 + e._021 + e._022, 2) 
/ 
power(e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022, 2)) *

(power(m._003, 2)+ power(m._004, 2)+ power(m._005, 2)+ power(m._006, 2)+ --
power(m._007, 2)+ power(m._008, 2)+ power(m._009, 2)+ power(m._010, 2)+
power(m._011, 2)+ power(m._014, 2)+ power(m._015, 2)+ power(m._016, 2)+
power(m._017, 2)+ power(m._018, 2)+ power(m._019, 2)+ power(m._020, 2)+
power(m._021, 2)+ power(m._022, 2)))

END as CB_ME_P,

--CB_3050_P------------------------------------------------------------------------------
CASE 
WHEN (e._008 + e._009 + e._010 + e._019 +
e._020 + e._021)=0 then 0.0

WHEN (e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022)=0 then 0.0

WHEN (e._008 + e._009 + e._010 + e._019 +
e._020 + e._021) is NULL then -.99999

WHEN (e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022) is NULL then -.99999

ELSE
cast(e._008 + e._009 + e._010 + e._019 +
e._020 + e._021 as FLOAT)*100 /
cast(e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022 as FLOAT)
END as CB_3050_P,

--CB_3050_ME_P---------------------------------------------------------------------------
CASE
WHEN (e._008 + e._009 + e._010 + e._019 +
e._020 + e._021) is NULL then -.99999

WHEN (e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022) is NULL then -.99999

when (e._008 + e._009 + e._010 + e._019 +
e._020 + e._021)=0 then 0

when (e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022) = 0 then 0

when /* (1/OWNOCCV2) * SQRT(CB_3050_ME^2-(CB^2/OWNOCCV2^2) * OWNOCCV2ME^2)*100 */
(power(m._008, 2) + power(m._009, 2) + power(m._010, 2) +
power(m._019, 2) + power(m._020, 2) + power(m._021, 2)) 
> 
(
power(e._008 + e._009 + e._010 + e._019 + e._020 + e._021, 2) 
/ 
power(e._003 + e._004 + e._005 + e._006 + e._007 + --OWNOCCV2^2
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022, 2)
)*
(power(m._003, 2)+ power(m._004, 2)+ power(m._005, 2)+ power(m._006, 2)+ --OWNOCCV2ME^2
power(m._007, 2)+ power(m._008, 2)+ power(m._009, 2)+ power(m._010, 2)+
power(m._011, 2)+ power(m._014, 2)+ power(m._015, 2)+ power(m._016, 2)+
power(m._017, 2)+ power(m._018, 2)+ power(m._019, 2)+ power(m._020, 2)+
power(m._021, 2)+ power(m._022, 2)) 
then
(100.0/ (e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022)) *

SQRT( (power(m._008, 2) + power(m._009, 2) + power(m._010, 2) +
power(m._019, 2) + power(m._020, 2) + power(m._021, 2) ) - 

(power(e._008 + e._009 + e._010 + e._019 +
e._020 + e._021, 2) 
/ 
power(e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022, 2)) *

(power(m._003, 2)+ power(m._004, 2)+ power(m._005, 2)+ power(m._006, 2)+ --
power(m._007, 2)+ power(m._008, 2)+ power(m._009, 2)+ power(m._010, 2)+
power(m._011, 2)+ power(m._014, 2)+ power(m._015, 2)+ power(m._016, 2)+
power(m._017, 2)+ power(m._018, 2)+ power(m._019, 2)+ power(m._020, 2)+
power(m._021, 2)+ power(m._022, 2)))

else /* (1/OWNOCCV2) * SQRT(CB_3050_ME^2+(CB^2/OWNOCCV2^2) * OWNOCCV2ME^2)*100 */
(100.0/ (e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022)) *

SQRT( (power(m._008, 2) + power(m._009, 2) + power(m._010, 2) +
power(m._019, 2) + power(m._020, 2) + power(m._021, 2) ) + 

(power(e._008 + e._009 + e._010 + e._019 +
e._020 + e._021, 2) 
/ 
power(e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022, 2)) *

(power(m._003, 2)+ power(m._004, 2)+ power(m._005, 2)+ power(m._006, 2)+ --
power(m._007, 2)+ power(m._008, 2)+ power(m._009, 2)+ power(m._010, 2)+
power(m._011, 2)+ power(m._014, 2)+ power(m._015, 2)+ power(m._016, 2)+
power(m._017, 2)+ power(m._018, 2)+ power(m._019, 2)+ power(m._020, 2)+
power(m._021, 2)+ power(m._022, 2)))

END as CB_3050_ME_P,

--CB_50_P--------------------------------------------------------------------------------
CASE 
WHEN (e._011 + e._022)=0 then 0.0

WHEN (e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022)=0 then 0.0

WHEN (e._011 + e._022) is NULL then -.99999

WHEN (e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022) is NULL then -.99999

ELSE
cast(e._011 + e._022 as FLOAT)*100 /
cast(e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022 as FLOAT)
END as CB_50_P,

--CB_50_ME_P-----------------------------------------------------------------------------
CASE
WHEN (e._011 + e._022) is NULL then -.99999

WHEN (e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022) is NULL then -.99999

when (e._011 + e._022)=0 then 0

when (e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022) = 0 then 0

when /* (1/OWNOCCV2) * SQRT(CB_50_ME^2-(CB_50^2/OWNOCCV2^2) * OWNOCCV2ME^2)*100 */
(power(m._011, 2) + power(m._022, 2)) 
> 
(
power(e._011 + e._022, 2) 
/ 
power(e._003 + e._004 + e._005 + e._006 + e._007 + --OWNOCCV2^2
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022, 2)
)*
(power(m._003, 2)+ power(m._004, 2)+ power(m._005, 2)+ power(m._006, 2)+ --OWNOCCV2ME^2
power(m._007, 2)+ power(m._008, 2)+ power(m._009, 2)+ power(m._010, 2)+
power(m._011, 2)+ power(m._014, 2)+ power(m._015, 2)+ power(m._016, 2)+
power(m._017, 2)+ power(m._018, 2)+ power(m._019, 2)+ power(m._020, 2)+
power(m._021, 2)+ power(m._022, 2)) 
then
(100.0/ (e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022)) *

SQRT( (power(m._011, 2) + power(m._022, 2)) - 

(power(e._011 + e._022, 2) 
/ 
power(e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022, 2)) *

(power(m._003, 2)+ power(m._004, 2)+ power(m._005, 2)+ power(m._006, 2)+ --
power(m._007, 2)+ power(m._008, 2)+ power(m._009, 2)+ power(m._010, 2)+
power(m._011, 2)+ power(m._014, 2)+ power(m._015, 2)+ power(m._016, 2)+
power(m._017, 2)+ power(m._018, 2)+ power(m._019, 2)+ power(m._020, 2)+
power(m._021, 2)+ power(m._022, 2)))

else /* (1/OWNOCCV2) * SQRT(CB_ME^2+(CB^2/OWNOCCV2^2) * OWNOCCV2ME^2)*100 */
(100.0/ (e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022)) *

SQRT( (power(m._011, 2) + power(m._022, 2)) + 

(power(e._011 + e._022, 2) 
/ 
power(e._003 + e._004 + e._005 + e._006 + e._007 + 
e._008 + e._009 + e._010 + e._011 + e._014 + 
e._015 + e._016 + e._017 + e._018 + e._019 + 
e._020 + e._021 + e._022, 2)) *

(power(m._003, 2)+ power(m._004, 2)+ power(m._005, 2)+ power(m._006, 2)+ --
power(m._007, 2)+ power(m._008, 2)+ power(m._009, 2)+ power(m._010, 2)+
power(m._011, 2)+ power(m._014, 2)+ power(m._015, 2)+ power(m._016, 2)+
power(m._017, 2)+ power(m._018, 2)+ power(m._019, 2)+ power(m._020, 2)+
power(m._021, 2)+ power(m._022, 2)))

END as CB_50_ME_P

--JOINS----------------------------------------------------------------------------------
from g20105ma g
join b25091_e e on e.logrecno = g.logrecno
join b25091_m m on m.logrecno = g.logrecno
where g.geoid like '06000%'
order by g.geoid;

