FAKE_OVERVIEW = """*SIMULATION OVERVIEW FILE

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:37

*RUN   1        : SAMPLE 1                  MZCER048 EXPEFILE    1
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT  1   : SAMPLE 1                  MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEAA   2021
 SOIL           : IB00000001     TEXTURE : yLoam -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:252.5mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.211 0.339 0.430 0.128 0.339   1.00   1.21   6.19   0.00   0.00   1.75
  5- 15 0.224 0.351 0.436 0.127 0.351   0.85   1.23   6.26   0.00   0.00   1.49
 15- 30 0.226 0.354 0.438 0.128 0.354   0.70   1.24   6.27   0.00   0.00   1.08
 30- 45 0.241 0.369 0.446 0.128 0.369   0.50   1.29   6.38   0.00   0.00   0.70
 45- 60 0.241 0.369 0.446 0.128 0.369   0.50   1.29   6.38   0.00   0.00   0.70
 60- 80 0.241 0.368 0.444 0.127 0.368   0.38   1.35   6.52   0.00   0.00   0.40
 80-100 0.241 0.368 0.444 0.127 0.368   0.38   1.35   6.52   0.00   0.00   0.40
100-150 0.231 0.356 0.437 0.125 0.356   0.05   1.41   6.69   0.00   0.00   0.23
150-200 0.231 0.356 0.437 0.125 0.356   0.05   1.41   6.69   0.00   0.00   0.23

TOT-200  46.7  71.9  87.9  25.3  71.9  <--cm   -  kg/ha-->    0.0    0.0 130123
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.     1     SAMPLE 1                

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  2 MAY    6 Emergence       16   0.00   2.2    1  4.4  0.00  0.01  0.00  0.00     1
 21 MAY   25 End Juveni     244   0.43  10.3    9  3.5  0.00  0.00  0.00  0.00     2
 26 MAY   30 Floral Ini     512   0.78  12.2   18  3.6  0.00  0.00  0.00  0.00     3
  5 JUL   70 75% Silkin    4361   1.64  25.4   51  1.2  0.28  0.31  0.00  0.00     4
 18 JUL   83 Beg Gr Fil    5929   1.53  25.4   90  1.5  0.11  0.10  0.00  0.00     5
  7 SEP  134 End Gr Fil    8954   1.20  25.4   91  1.0  0.00  0.00  0.00  0.00     6
 10 SEP  137 Maturity      8954   1.20  25.4   91  1.0  0.00  0.03  0.00  0.00    10
 10 SEP  137 Harvest       8954   1.20  25.4   91  1.0  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                      70          -99
      Physiological maturity day (dap)                       137          -99
      Yield at harvest maturity (kg [dm]/ha)                3086          -99
      Number at maturity (no/m2)                            1008          -99
      Unit wt at maturity (g [dm]/unit)                   0.3060          -99
      Number at maturity (no/unit)                         252.1          -99
      Tops weight at maturity (kg [dm]/ha)                  8954          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    5919          -99
      Leaf area index, maximum                              2.28          -99
      Harvest index at maturity                            0.345          -99
      Grain N at maturity (kg/ha)                             52          -99
      Tops N at maturity (kg/ha)                              91          -99
      Stem N at maturity (kg/ha)                              39          -99
      Grain N at maturity (%)                                1.7          -99
      Tops weight at anthesis (kg [dm]/ha)                  4290          -99
      Tops N at anthesis (kg/ha)                              51          -99
      Leaf number per stem at maturity                     25.39          -99
      Emergence day (dap)                                      6          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    19  26.9  18.6  22.8  19.5  12.03  380.0  152.1   56.7   97.7    0    0    1    0    0   12  0.000  0.000  0.002  0.004  0.000  0.000
 End Juvenil-Floral Init    5  27.6  18.4  23.0  21.6  12.04  380.0   12.0   12.0   27.3    0    0    0    0    0    3  0.000  0.000  0.000  0.001  0.000  0.000
 Floral Init-End Lf Grow   40  28.3  17.8  23.1  21.2  12.04  380.0    3.6  121.4  200.6    0    0    0    0    0    9  0.194  0.258  0.130  0.294  0.000  0.000
 End Lf Grth-Beg Grn Fil   13  25.2  17.7  21.4  15.3  12.04  380.0  122.9   38.5   46.0    0    0    0    0    0   13  0.129  0.172  0.061  0.139  0.000  0.000
 Grain Filling Phase       51  26.5  17.5  22.0  19.4  12.02  380.0  454.5  219.2  232.6    0    0    0    0    0   39  0.000  0.000  0.000  0.000  0.000  0.000
   
 Planting to Harvest      137  27.2  17.8  22.5  19.8  12.03  380.0  752.9  469.2  657.2    0    0    4    0    0   79  0.069  0.092  0.044  0.101  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 137 days 

 Precipitation during growing season       752.9 mm[rain]
   Dry Matter Productivity                  1.19 kg[DM]/m3[rain]          =   11.9 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.41 kg[grain yield]/m3[rain] =    4.1 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  469.2 mm[ET]
   Dry Matter Productivity                  1.91 kg[DM]/m3[ET]            =   19.1 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.66 kg[grain yield]/m3[ET]   =    6.6 kg[yield]/ha per mm[ET]

 Transpiration during growing season       309.7 mm[EP]
   Dry Matter Productivity                  2.89 kg[DM]/m3[EP]            =   28.9 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.00 kg[grain yield]/m3[EP]   =   10.0 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                  59.7 kg[DM]/kg[N applied]
   Yield Productivity                       20.6 kg[yield]/kg[N applied]

 N uptake during growing season              143 kg[N uptake]/ha
   Dry Matter Productivity                  62.6 kg[DM]/kg[N uptake]
   Yield Productivity                       21.6 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     3086 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:38

*RUN   2        : SAMPLE 2                  MZCER048 EXPEFILE    2
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT  2   : SAMPLE 2                  MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEAB   2021
 SOIL           : IB00000002     TEXTURE : ClayL -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:248.4mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.204 0.329 0.424 0.125 0.329   1.00   1.24   6.34   0.00   0.00   1.54
  5- 15 0.218 0.344 0.431 0.126 0.344   0.85   1.26   6.41   0.00   0.00   1.30
 15- 30 0.218 0.343 0.431 0.125 0.343   0.70   1.27   6.44   0.00   0.00   0.93
 30- 45 0.233 0.359 0.439 0.126 0.359   0.50   1.32   6.55   0.00   0.00   0.59
 45- 60 0.233 0.359 0.439 0.126 0.359   0.50   1.32   6.55   0.00   0.00   0.59
 60- 80 0.233 0.358 0.438 0.125 0.358   0.38   1.37   6.68   0.00   0.00   0.34
 80-100 0.233 0.358 0.438 0.125 0.358   0.38   1.37   6.68   0.00   0.00   0.34
100-150 0.222 0.345 0.430 0.123 0.345   0.05   1.43   6.86   0.00   0.00   0.21
150-200 0.222 0.345 0.430 0.123 0.345   0.05   1.43   6.86   0.00   0.00   0.21

TOT-200  45.0  69.8  86.6  24.8  69.8  <--cm   -  kg/ha-->    0.0    0.0 115671
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.     2     SAMPLE 2                

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  5 MAY    9 Emergence       16   0.00   1.8    1  4.4  0.00  0.00  0.00  0.00     1
 10 JUN   45 End Juveni     226   0.41  10.1    9  3.8  0.00  0.01  0.00  0.00     2
 15 JUN   50 Floral Ini     347   0.57  11.2   13  3.6  0.00  0.00  0.00  0.00     3
  7 SEP  134 75% Silkin    5132   1.59  23.5   61  1.2  0.00  0.18  0.00  0.00     4
  1 OCT  158 Beg Gr Fil    8037   0.81  23.5   61  0.8  0.00  0.56  0.00  0.00     5
 28 DEC  246 End Gr Fil   10591   0.05  23.5   65  0.6  0.00  0.62  0.00  0.00     6
  3 JAN  252 Maturity     10591   0.05  23.5   65  0.6  0.00  0.59  0.00  0.00    10
  3 JAN  252 Harvest      10591   0.05  23.5   65  0.6  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     134          -99
      Physiological maturity day (dap)                       252          -99
      Yield at harvest maturity (kg [dm]/ha)                3291          -99
      Number at maturity (no/m2)                             629          -99
      Unit wt at maturity (g [dm]/unit)                   0.5235          -99
      Number at maturity (no/unit)                         157.2          -99
      Tops weight at maturity (kg [dm]/ha)                 10591          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    7352          -99
      Leaf area index, maximum                              2.49          -99
      Harvest index at maturity                            0.311          -99
      Grain N at maturity (kg/ha)                             29          -99
      Tops N at maturity (kg/ha)                              65          -99
      Stem N at maturity (kg/ha)                              35          -99
      Grain N at maturity (%)                                0.9          -99
      Tops weight at anthesis (kg [dm]/ha)                  4936          -99
      Tops N at anthesis (kg/ha)                              61          -99
      Leaf number per stem at maturity                     23.45          -99
      Emergence day (dap)                                      9          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    36  19.7  11.4  15.5  20.8  12.05  380.0  367.2   99.3  169.5    0    0    0    0    0   27  0.000  0.000  0.003  0.006  0.000  0.000
 End Juvenil-Floral Init    5  20.5  11.1  15.8  22.1  12.05  380.0    3.0    8.9   24.2    0    0    0    0    0    4  0.000  0.000  0.000  0.000  0.000  0.000
 Floral Init-End Lf Grow   84  18.7  10.7  14.7  19.0  12.04  380.0   1654    308    321    0    0    0    0    0   81  0.000  0.000  0.070  0.175  0.000  0.000
 End Lf Grth-Beg Grn Fil   24  19.5  10.8  15.2  21.3  12.00  380.0  469.0  105.2  106.2    0    0    0    0    0   24  0.000  0.000  0.265  0.550  0.000  0.000
 Grain Filling Phase       88  20.7  11.3  16.0  22.8  11.96  380.0  691.4  219.0  449.0    0    0    0    0    0   70  0.000  0.000  0.343  0.619  0.000  0.000
   
 Planting to Harvest      252  19.8  11.1  15.5  21.1  12.01  380.0   3286    768   1149    0    0    0    0    0  216  0.000  0.000  0.176  0.342  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 252 days 

 Precipitation during growing season      3285.8 mm[rain]
   Dry Matter Productivity                  0.32 kg[DM]/m3[rain]          =    3.2 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.10 kg[grain yield]/m3[rain] =    1.0 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  767.8 mm[ET]
   Dry Matter Productivity                  1.38 kg[DM]/m3[ET]            =   13.8 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.43 kg[grain yield]/m3[ET]   =    4.3 kg[yield]/ha per mm[ET]

 Transpiration during growing season       364.9 mm[EP]
   Dry Matter Productivity                  2.90 kg[DM]/m3[EP]            =   29.0 kg[DM]/ha per mm[EP]
   Yield Productivity                       0.90 kg[grain yield]/m3[EP]   =    9.0 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                  70.6 kg[DM]/kg[N applied]
   Yield Productivity                       21.9 kg[yield]/kg[N applied]

 N uptake during growing season              134 kg[N uptake]/ha
   Dry Matter Productivity                  79.0 kg[DM]/kg[N uptake]
   Yield Productivity                       24.6 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     3291 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:38

*RUN   3        : SAMPLE 3                  MZCER048 EXPEFILE    3
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT  3   : SAMPLE 3                  MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEAC   2021
 SOIL           : IB00000003     TEXTURE : ClayL -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:245.1mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.203 0.326 0.422 0.123 0.326   1.00   1.22   6.12   0.00   0.00   2.12
  5- 15 0.219 0.343 0.431 0.124 0.343   0.85   1.24   6.20   0.00   0.00   1.79
 15- 30 0.219 0.342 0.430 0.123 0.342   0.70   1.25   6.17   0.00   0.00   1.26
 30- 45 0.237 0.362 0.440 0.125 0.362   0.50   1.30   6.29   0.00   0.00   0.80
 45- 60 0.237 0.362 0.440 0.125 0.362   0.50   1.30   6.29   0.00   0.00   0.80
 60- 80 0.236 0.360 0.438 0.124 0.360   0.38   1.36   6.41   0.00   0.00   0.47
 80-100 0.236 0.360 0.438 0.124 0.360   0.38   1.36   6.41   0.00   0.00   0.47
100-150 0.224 0.345 0.430 0.121 0.345   0.05   1.41   6.59   0.00   0.00   0.27
150-200 0.224 0.345 0.430 0.121 0.345   0.05   1.41   6.59   0.00   0.00   0.27

TOT-200  45.4  69.9  86.6  24.5  69.9  <--cm   -  kg/ha-->    0.0    0.0 153591
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.     3     SAMPLE 3                

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  5 MAY    9 Emergence       16   0.00   1.9    1  4.4  0.00  0.00  0.00  0.00     1
  8 JUN   43 End Juveni     219   0.40  10.0    8  3.8  0.00  0.00  0.00  0.00     2
 13 JUN   48 Floral Ini     346   0.57  11.2   12  3.6  0.00  0.00  0.00  0.00     3
  1 SEP  128 75% Silkin    5413   2.04  23.5   72  1.3  0.00  0.12  0.00  0.00     4
 24 SEP  151 Beg Gr Fil    9226   1.19  23.5   72  0.8  0.00  0.46  0.00  0.00     5
 15 DEC  233 End Gr Fil   13037   0.12  23.5   82  0.6  0.00  0.56  0.00  0.00     6
 20 DEC  238 Maturity     13037   0.12  23.5   82  0.6  0.00  0.59  0.00  0.00    10
 20 DEC  238 Harvest      13037   0.12  23.5   82  0.6  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     128          -99
      Physiological maturity day (dap)                       238          -99
      Yield at harvest maturity (kg [dm]/ha)                4282          -99
      Number at maturity (no/m2)                             867          -99
      Unit wt at maturity (g [dm]/unit)                   0.4938          -99
      Number at maturity (no/unit)                         216.8          -99
      Tops weight at maturity (kg [dm]/ha)                 13037          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    8808          -99
      Leaf area index, maximum                              2.60          -99
      Harvest index at maturity                            0.328          -99
      Grain N at maturity (kg/ha)                             42          -99
      Tops N at maturity (kg/ha)                              82          -99
      Stem N at maturity (kg/ha)                              40          -99
      Grain N at maturity (%)                                1.0          -99
      Tops weight at anthesis (kg [dm]/ha)                  5215          -99
      Tops N at anthesis (kg/ha)                              72          -99
      Leaf number per stem at maturity                     23.49          -99
      Emergence day (dap)                                      9          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    34  20.1  11.8  15.9  20.4  12.05  380.0  367.2   96.4  158.6    0    0    0    0    0   27  0.000  0.000  0.002  0.006  0.000  0.000
 End Juvenil-Floral Init    5  20.9  11.2  16.0  22.6  12.05  380.0    1.2    8.9   24.9    0    0    0    0    0    2  0.000  0.000  0.000  0.000  0.000  0.000
 Floral Init-End Lf Grow   80  19.1  11.0  15.0  18.9  12.04  380.0   1419    289    306    0    0    0    0    0   77  0.000  0.000  0.047  0.118  0.000  0.000
 End Lf Grth-Beg Grn Fil   23  19.7  11.3  15.5  20.2  12.01  380.0  592.8   94.7   95.2    0    0    0    0    0   23  0.000  0.000  0.183  0.452  0.000  0.000
 Grain Filling Phase       82  21.3  12.1  16.7  22.8  11.97  380.0  794.1  249.0  418.1    0    0    0    0    0   67  0.000  0.000  0.274  0.562  0.000  0.000
   
 Planting to Harvest      238  20.3  11.6  15.9  20.9  12.01  380.0   3275    768   1077    0    0    0    0    0  205  0.000  0.000  0.135  0.290  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 238 days 

 Precipitation during growing season      3274.5 mm[rain]
   Dry Matter Productivity                  0.40 kg[DM]/m3[rain]          =    4.0 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.13 kg[grain yield]/m3[rain] =    1.3 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  768.2 mm[ET]
   Dry Matter Productivity                  1.70 kg[DM]/m3[ET]            =   17.0 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.56 kg[grain yield]/m3[ET]   =    5.6 kg[yield]/ha per mm[ET]

 Transpiration during growing season       399.8 mm[EP]
   Dry Matter Productivity                  3.26 kg[DM]/m3[EP]            =   32.6 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.07 kg[grain yield]/m3[EP]   =   10.7 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                  86.9 kg[DM]/kg[N applied]
   Yield Productivity                       28.5 kg[yield]/kg[N applied]

 N uptake during growing season              159 kg[N uptake]/ha
   Dry Matter Productivity                  82.0 kg[DM]/kg[N uptake]
   Yield Productivity                       26.9 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     4282 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:38

*RUN   4        : SAMPLE 4                  MZCER048 EXPEFILE    4
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT  4   : SAMPLE 4                  MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEAD   2021
 SOIL           : IB00000004     TEXTURE : ClayL -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:244.6mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.202 0.326 0.422 0.124 0.326   1.00   1.25   6.35   0.00   0.00   1.56
  5- 15 0.216 0.340 0.429 0.124 0.340   0.85   1.27   6.43   0.00   0.00   1.31
 15- 30 0.216 0.340 0.428 0.124 0.340   0.70   1.28   6.43   0.00   0.00   0.93
 30- 45 0.231 0.355 0.436 0.124 0.355   0.50   1.33   6.53   0.00   0.00   0.60
 45- 60 0.231 0.355 0.436 0.124 0.355   0.50   1.33   6.53   0.00   0.00   0.60
 60- 80 0.232 0.355 0.435 0.123 0.355   0.38   1.39   6.66   0.00   0.00   0.35
 80-100 0.232 0.355 0.435 0.123 0.355   0.38   1.39   6.66   0.00   0.00   0.35
100-150 0.220 0.341 0.428 0.121 0.341   0.05   1.43   6.85   0.00   0.00   0.20
150-200 0.220 0.341 0.428 0.121 0.341   0.05   1.43   6.85   0.00   0.00   0.20

TOT-200  44.6  69.1  86.1  24.5  69.1  <--cm   -  kg/ha-->    0.0    0.0 116243
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.     4     SAMPLE 4                

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  2 MAY    6 Emergence       16   0.00   2.1    1  4.4  0.00  0.01  0.00  0.00     1
 26 MAY   30 End Juveni     276   0.48  10.6   10  3.6  0.00  0.00  0.00  0.00     2
 31 MAY   35 Floral Ini     518   0.79  12.3   19  3.6  0.00  0.00  0.00  0.00     3
 20 JUL   85 75% Silkin    4819   1.72  25.4   79  1.6  0.17  0.27  0.00  0.00     4
  5 AUG  101 Beg Gr Fil    7291   1.60  25.4   79  1.1  0.00  0.09  0.00  0.00     5
  3 OCT  160 End Gr Fil   11508   0.76  25.4   86  0.7  0.00  0.26  0.00  0.00     6
  7 OCT  164 Maturity     11508   0.76  25.4   86  0.7  0.00  0.64  0.00  0.00    10
  7 OCT  164 Harvest      11508   0.76  25.4   86  0.7  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                      85          -99
      Physiological maturity day (dap)                       164          -99
      Yield at harvest maturity (kg [dm]/ha)                4289          -99
      Number at maturity (no/m2)                            1212          -99
      Unit wt at maturity (g [dm]/unit)                   0.3540          -99
      Number at maturity (no/unit)                         302.9          -99
      Tops weight at maturity (kg [dm]/ha)                 11508          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    7269          -99
      Leaf area index, maximum                              1.75          -99
      Harvest index at maturity                            0.373          -99
      Grain N at maturity (kg/ha)                             59          -99
      Tops N at maturity (kg/ha)                              86          -99
      Stem N at maturity (kg/ha)                              27          -99
      Grain N at maturity (%)                                1.4          -99
      Tops weight at anthesis (kg [dm]/ha)                  4636          -99
      Tops N at anthesis (kg/ha)                              79          -99
      Leaf number per stem at maturity                     25.42          -99
      Emergence day (dap)                                      6          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    24  24.6  15.9  20.2  20.1  12.04  380.0  176.1   64.4  120.6    0    0    0    0    0   15  0.000  0.000  0.002  0.005  0.000  0.000
 End Juvenil-Floral Init    5  25.7  15.6  20.7  22.5  12.05  380.0    0.2   11.7   27.1    0    0    0    0    0    1  0.000  0.000  0.001  0.002  0.000  0.000
 Floral Init-End Lf Grow   50  25.1  15.1  20.1  19.1  12.05  380.0  203.0  140.2  216.3    0    0    0    0    0   23  0.126  0.174  0.117  0.264  0.000  0.000
 End Lf Grth-Beg Grn Fil   16  22.3  14.8  18.6  16.0  12.04  380.0  247.8   55.7   56.1    0    0    0    0    0   14  0.000  0.000  0.038  0.096  0.000  0.000
 Grain Filling Phase       59  25.2  14.8  20.0  21.7  12.02  380.0  186.5  257.8  291.7    0    0    0    0    0   42  0.000  0.000  0.107  0.250  0.000  0.000
   
 Planting to Harvest      164  24.9  15.1  20.0  20.2  12.03  380.0  818.4  552.1  769.3    0    0    0    0    0  100  0.038  0.053  0.087  0.196  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 164 days 

 Precipitation during growing season       818.4 mm[rain]
   Dry Matter Productivity                  1.41 kg[DM]/m3[rain]          =   14.1 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.52 kg[grain yield]/m3[rain] =    5.2 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  552.1 mm[ET]
   Dry Matter Productivity                  2.08 kg[DM]/m3[ET]            =   20.8 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.78 kg[grain yield]/m3[ET]   =    7.8 kg[yield]/ha per mm[ET]

 Transpiration during growing season       353.3 mm[EP]
   Dry Matter Productivity                  3.26 kg[DM]/m3[EP]            =   32.6 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.21 kg[grain yield]/m3[EP]   =   12.1 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                  76.7 kg[DM]/kg[N applied]
   Yield Productivity                       28.6 kg[yield]/kg[N applied]

 N uptake during growing season              135 kg[N uptake]/ha
   Dry Matter Productivity                  85.2 kg[DM]/kg[N uptake]
   Yield Productivity                       31.8 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     4289 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:38

*RUN   5        : SAMPLE 5                  MZCER048 EXPEFILE    5
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT  5   : SAMPLE 5                  MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEAE   2021
 SOIL           : IB00000005     TEXTURE : Loam  -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:248.2mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.199 0.322 0.421 0.123 0.322   1.00   1.16   5.73   0.00   0.00   3.14
  5- 15 0.217 0.342 0.430 0.125 0.342   0.85   1.18   5.80   0.00   0.00   2.65
 15- 30 0.219 0.344 0.431 0.125 0.344   0.70   1.19   5.76   0.00   0.00   2.00
 30- 45 0.240 0.366 0.443 0.126 0.366   0.50   1.24   5.87   0.00   0.00   1.28
 45- 60 0.240 0.366 0.443 0.126 0.366   0.50   1.24   5.87   0.00   0.00   1.28
 60- 80 0.239 0.364 0.441 0.125 0.364   0.38   1.33   6.00   0.00   0.00   0.75
 80-100 0.239 0.364 0.441 0.125 0.364   0.38   1.33   6.00   0.00   0.00   0.75
100-150 0.224 0.347 0.431 0.123 0.347   0.05   1.39   6.18   0.00   0.00   0.42
150-200 0.224 0.347 0.431 0.123 0.347   0.05   1.39   6.18   0.00   0.00   0.42

TOT-200  45.6  70.4  86.9  24.8  70.4  <--cm   -  kg/ha-->    0.0    0.0 231078
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.     5     SAMPLE 5                

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 27 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 28 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  7 MAY   10 Emergence       16   0.00   1.8    1  4.4  0.00  0.00  0.00  0.00     1
 15 JUN   49 End Juveni     223   0.40  10.1    9  3.9  0.00  0.01  0.00  0.00     2
 20 JUN   54 Floral Ini     315   0.53  11.0   11  3.6  0.00  0.00  0.00  0.00     3
 18 SEP  144 75% Silkin    3954   1.03  23.0   43  1.1  0.00  0.24  0.00  0.00     4
 11 OCT  167 Beg Gr Fil    5727   0.53  23.0   44  0.8  0.00  0.57  0.00  0.00     5
 11 JAN  259 End Gr Fil    7976   0.07  23.0   64  0.8  0.00  0.45  0.00  0.00     6
 16 JAN  264 Maturity      7976   0.07  23.0   64  0.8  0.00  0.15  0.00  0.00    10
 16 JAN  264 Harvest       7976   0.07  23.0   64  0.8  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     144          -99
      Physiological maturity day (dap)                       264          -99
      Yield at harvest maturity (kg [dm]/ha)                2530          -99
      Number at maturity (no/m2)                             469          -99
      Unit wt at maturity (g [dm]/unit)                   0.5396          -99
      Number at maturity (no/unit)                         117.2          -99
      Tops weight at maturity (kg [dm]/ha)                  7976          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    5488          -99
      Leaf area index, maximum                              1.78          -99
      Harvest index at maturity                            0.317          -99
      Grain N at maturity (kg/ha)                             28          -99
      Tops N at maturity (kg/ha)                              64          -99
      Stem N at maturity (kg/ha)                              36          -99
      Grain N at maturity (%)                                1.1          -99
      Tops weight at anthesis (kg [dm]/ha)                  3909          -99
      Tops N at anthesis (kg/ha)                              43          -99
      Leaf number per stem at maturity                     23.01          -99
      Emergence day (dap)                                     10          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    39  19.6  10.1  14.9  19.4  12.06  380.0  518.4  104.9  169.1    0    0    0    0    0   34  0.000  0.000  0.002  0.006  0.000  0.000
 End Juvenil-Floral Init    5  18.4  10.4  14.4  17.0  12.06  380.0   87.6   17.6   18.2    0    0    0    0    0    5  0.000  0.000  0.000  0.000  0.000  0.000
 Floral Init-End Lf Grow   90  18.4   9.9  14.2  17.7  12.04  380.0   3431    315    321    0    0    0    0    0   88  0.000  0.000  0.093  0.233  0.000  0.000
 End Lf Grth-Beg Grn Fil   23  19.9  10.4  15.2  19.4  11.99  380.0   1211     95     96    0    0    0    0    0   23  0.000  0.000  0.275  0.563  0.000  0.000
 Grain Filling Phase       92  21.4  10.1  15.8  21.3  11.95  380.0  506.7  266.2  443.1    0    0    0    0    0   70  0.000  0.000  0.200  0.455  0.000  0.000
   
 Planting to Harvest      264  20.0  10.1  15.0  19.6  12.01  380.0   6268    835   1123    0    0    0    0    0  231  0.000  0.000  0.127  0.291  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 264 days 

 Precipitation during growing season      6267.6 mm[rain]
   Dry Matter Productivity                  0.13 kg[DM]/m3[rain]          =    1.3 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.04 kg[grain yield]/m3[rain] =    0.4 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  835.1 mm[ET]
   Dry Matter Productivity                  0.96 kg[DM]/m3[ET]            =    9.6 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.30 kg[grain yield]/m3[ET]   =    3.0 kg[yield]/ha per mm[ET]

 Transpiration during growing season       298.5 mm[EP]
   Dry Matter Productivity                  2.67 kg[DM]/m3[EP]            =   26.7 kg[DM]/ha per mm[EP]
   Yield Productivity                       0.85 kg[grain yield]/m3[EP]   =    8.5 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                  53.2 kg[DM]/kg[N applied]
   Yield Productivity                       16.9 kg[yield]/kg[N applied]

 N uptake during growing season              129 kg[N uptake]/ha
   Dry Matter Productivity                  61.8 kg[DM]/kg[N uptake]
   Yield Productivity                       19.6 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     2530 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:38

*RUN   6        : SAMPLE 6                  MZCER048 EXPEFILE    6
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT  6   : SAMPLE 6                  MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEAF   2021
 SOIL           : IB00000004     TEXTURE : ClayL -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:244.6mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.202 0.326 0.422 0.124 0.326   1.00   1.25   6.35   0.00   0.00   1.56
  5- 15 0.216 0.340 0.429 0.124 0.340   0.85   1.27   6.43   0.00   0.00   1.31
 15- 30 0.216 0.340 0.428 0.124 0.340   0.70   1.28   6.43   0.00   0.00   0.93
 30- 45 0.231 0.355 0.436 0.124 0.355   0.50   1.33   6.53   0.00   0.00   0.60
 45- 60 0.231 0.355 0.436 0.124 0.355   0.50   1.33   6.53   0.00   0.00   0.60
 60- 80 0.232 0.355 0.435 0.123 0.355   0.38   1.39   6.66   0.00   0.00   0.35
 80-100 0.232 0.355 0.435 0.123 0.355   0.38   1.39   6.66   0.00   0.00   0.35
100-150 0.220 0.341 0.428 0.121 0.341   0.05   1.43   6.85   0.00   0.00   0.20
150-200 0.220 0.341 0.428 0.121 0.341   0.05   1.43   6.85   0.00   0.00   0.20

TOT-200  44.6  69.1  86.1  24.5  69.1  <--cm   -  kg/ha-->    0.0    0.0 116243
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.     6     SAMPLE 6                

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 30 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
  1 MAY    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  9 MAY    9 Emergence       16   0.00   1.8    1  4.4  0.00  0.00  0.00  0.00     1
  9 JUN   40 End Juveni     240   0.43  10.3    9  3.8  0.00  0.01  0.00  0.00     2
 14 JUN   45 Floral Ini     383   0.62  11.5   14  3.6  0.00  0.00  0.00  0.00     3
 25 AUG  117 75% Silkin    6397   2.88  23.9   95  1.5  0.00  0.06  0.00  0.00     4
 14 SEP  137 Beg Gr Fil   10708   2.15  23.9   96  0.9  0.00  0.29  0.00  0.00     5
  3 DEC  217 End Gr Fil   16583   0.23  23.9  101  0.6  0.00  0.56  0.00  0.00     6
  8 DEC  222 Maturity     16583   0.23  23.9  101  0.6  0.00  0.75  0.00  0.00    10
  8 DEC  222 Harvest      16583   0.23  23.9  101  0.6  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     117          -99
      Physiological maturity day (dap)                       222          -99
      Yield at harvest maturity (kg [dm]/ha)                6662          -99
      Number at maturity (no/m2)                            1387          -99
      Unit wt at maturity (g [dm]/unit)                   0.4803          -99
      Number at maturity (no/unit)                         346.7          -99
      Tops weight at maturity (kg [dm]/ha)                 16583          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    9978          -99
      Leaf area index, maximum                              3.06          -99
      Harvest index at maturity                            0.402          -99
      Grain N at maturity (kg/ha)                             65          -99
      Tops N at maturity (kg/ha)                             101          -99
      Stem N at maturity (kg/ha)                              36          -99
      Grain N at maturity (%)                                1.0          -99
      Tops weight at anthesis (kg [dm]/ha)                  6098          -99
      Tops N at anthesis (kg/ha)                              95          -99
      Leaf number per stem at maturity                     23.93          -99
      Emergence day (dap)                                      9          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    31  22.0  11.8  16.9  19.7  12.06  380.0  148.9  106.8  143.1    0    0    0    0    0   30  0.000  0.000  0.003  0.007  0.000  0.000
 End Juvenil-Floral Init    5  22.3  11.5  16.9  20.9  12.06  380.0    3.8   10.9   23.4    0    0    0    0    0    5  0.000  0.000  0.000  0.001  0.000  0.000
 Floral Init-End Lf Grow   72  20.4  11.5  15.9  18.1  12.05  380.0  548.8  259.0  267.3    0    0    0    0    0   72  0.000  0.000  0.023  0.059  0.000  0.000
 End Lf Grth-Beg Grn Fil   20  20.8  11.8  16.3  19.2  12.02  380.0  309.1   77.5   77.9    0    0    0    0    0   19  0.000  0.000  0.115  0.288  0.000  0.000
 Grain Filling Phase       80  22.3  11.6  17.0  20.8  11.97  380.0  936.0  326.6  365.9    0    0    0    0    0   80  0.000  0.000  0.293  0.555  0.000  0.000
   
 Planting to Harvest      222  21.5  11.6  16.6  19.6  12.02  380.0   2211    829    943    0    0    0    0    0  219  0.000  0.000  0.135  0.263  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 222 days 

 Precipitation during growing season      2210.9 mm[rain]
   Dry Matter Productivity                  0.75 kg[DM]/m3[rain]          =    7.5 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.30 kg[grain yield]/m3[rain] =    3.0 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  828.6 mm[ET]
   Dry Matter Productivity                  2.00 kg[DM]/m3[ET]            =   20.0 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.80 kg[grain yield]/m3[ET]   =    8.0 kg[yield]/ha per mm[ET]

 Transpiration during growing season       438.4 mm[EP]
   Dry Matter Productivity                  3.78 kg[DM]/m3[EP]            =   37.8 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.52 kg[grain yield]/m3[EP]   =   15.2 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                 110.6 kg[DM]/kg[N applied]
   Yield Productivity                       44.4 kg[yield]/kg[N applied]

 N uptake during growing season              174 kg[N uptake]/ha
   Dry Matter Productivity                  95.3 kg[DM]/kg[N uptake]
   Yield Productivity                       38.3 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     6662 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:38

*RUN   7        : SAMPLE 7                  MZCER048 EXPEFILE    7
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT  7   : SAMPLE 7                  MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEAG   2021
 SOIL           : IB00000006     TEXTURE : ClayL -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:243.3mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.203 0.325 0.422 0.122 0.325   1.00   1.23   6.19   0.00   0.00   1.96
  5- 15 0.219 0.343 0.430 0.124 0.343   0.85   1.25   6.25   0.00   0.00   1.64
 15- 30 0.219 0.342 0.429 0.123 0.342   0.70   1.26   6.21   0.00   0.00   1.15
 30- 45 0.236 0.360 0.438 0.124 0.360   0.50   1.31   6.32   0.00   0.00   0.74
 45- 60 0.236 0.360 0.438 0.124 0.360   0.50   1.31   6.32   0.00   0.00   0.74
 60- 80 0.236 0.359 0.437 0.123 0.359   0.38   1.37   6.45   0.00   0.00   0.43
 80-100 0.236 0.359 0.437 0.123 0.359   0.38   1.37   6.45   0.00   0.00   0.43
100-150 0.223 0.343 0.429 0.120 0.343   0.05   1.42   6.63   0.00   0.00   0.24
150-200 0.223 0.343 0.429 0.120 0.343   0.05   1.42   6.63   0.00   0.00   0.24

TOT-200  45.3  69.6  86.4  24.3  69.6  <--cm   -  kg/ha-->    0.0    0.0 141015
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.     7     SAMPLE 7                

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 27 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 28 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  9 MAY   12 Emergence       16   0.00   1.7    1  4.4  0.00  0.00  0.00  0.00     1
 26 JUN   60 End Juveni     217   0.39  10.0    9  4.0  0.00  0.01  0.00  0.00     2
  1 JUL   65 Floral Ini     291   0.49  10.8   11  3.6  0.00  0.00  0.00  0.00     3
 16 OCT  172 75% Silkin    2757   0.35  22.7   23  0.8  0.00  0.42  0.00  0.00     4
 14 NOV  201 Beg Gr Fil    3043   0.13  22.7   26  0.8  0.00  0.68  0.00  0.00     5
  5 MAR  312 End Gr Fil    3935   0.02  22.7   36  0.9  0.00  0.38  0.00  0.00     6
 11 MAR  318 Maturity      3935   0.02  22.7   36  0.9  0.00  0.00  0.00  0.00    10
 11 MAR  318 Harvest       3935   0.02  22.7   36  0.9  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     172          -99
      Physiological maturity day (dap)                       318          -99
      Yield at harvest maturity (kg [dm]/ha)                1320          -99
      Number at maturity (no/m2)                             233          -99
      Unit wt at maturity (g [dm]/unit)                   0.5672          -99
      Number at maturity (no/unit)                          58.2          -99
      Tops weight at maturity (kg [dm]/ha)                  3935          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    2649          -99
      Leaf area index, maximum                              1.16          -99
      Harvest index at maturity                            0.335          -99
      Grain N at maturity (kg/ha)                             16          -99
      Tops N at maturity (kg/ha)                              36          -99
      Stem N at maturity (kg/ha)                              20          -99
      Grain N at maturity (%)                                1.2          -99
      Tops weight at anthesis (kg [dm]/ha)                  2723          -99
      Tops N at anthesis (kg/ha)                              23          -99
      Leaf number per stem at maturity                     22.66          -99
      Emergence day (dap)                                     12          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    48  17.7   9.2  13.5  21.0  12.05  380.0  574.9  130.2  217.5    0    0    0    0    0   41  0.000  0.000  0.002  0.006  0.000  0.000
 End Juvenil-Floral Init    5  17.2   8.6  12.9  21.7  12.06  380.0   13.3   20.8   22.4    0    0    0    0    0    5  0.000  0.000  0.000  0.001  0.000  0.000
 Floral Init-End Lf Grow  107  17.0   9.3  13.1  19.5  12.02  380.0   4881    419    424    0    0    0    0    0  107  0.000  0.000  0.226  0.411  0.000  0.000
 End Lf Grth-Beg Grn Fil   29  18.0   9.7  13.8  23.3  11.96  380.0  103.1   92.3  145.2    0    0    0    0    0   29  0.000  0.000  0.417  0.680  0.000  0.000
 Grain Filling Phase      111  19.1   9.7  14.4  23.4  11.95  380.0  479.6  229.7  577.0    0    0    0    0    0   73  0.000  0.000  0.169  0.387  0.000  0.000
   
 Planting to Harvest      318  18.1   9.5  13.8  21.7  12.00  380.0   6581    937   1481    0    0    0    0    0  267  0.000  0.000  0.173  0.336  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 318 days 

 Precipitation during growing season      6581.3 mm[rain]
   Dry Matter Productivity                  0.06 kg[DM]/m3[rain]          =    0.6 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.02 kg[grain yield]/m3[rain] =    0.2 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  936.6 mm[ET]
   Dry Matter Productivity                  0.42 kg[DM]/m3[ET]            =    4.2 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.14 kg[grain yield]/m3[ET]   =    1.4 kg[yield]/ha per mm[ET]

 Transpiration during growing season       233.4 mm[EP]
   Dry Matter Productivity                  1.69 kg[DM]/m3[EP]            =   16.9 kg[DM]/ha per mm[EP]
   Yield Productivity                       0.57 kg[grain yield]/m3[EP]   =    5.7 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                  26.2 kg[DM]/kg[N applied]
   Yield Productivity                        8.8 kg[yield]/kg[N applied]

 N uptake during growing season               81 kg[N uptake]/ha
   Dry Matter Productivity                  48.6 kg[DM]/kg[N uptake]
   Yield Productivity                       16.3 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     1320 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:38

*RUN   8        : SAMPLE 8                  MZCER048 EXPEFILE    8
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT  8   : SAMPLE 8                  MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEAH   2021
 SOIL           : IB00000007     TEXTURE : Loam  -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:248.6mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.204 0.327 0.423 0.123 0.327   1.00   1.17   5.74   0.00   0.00   2.89
  5- 15 0.221 0.346 0.432 0.125 0.346   0.85   1.19   5.81   0.00   0.00   2.45
 15- 30 0.223 0.348 0.433 0.125 0.348   0.70   1.20   5.78   0.00   0.00   1.82
 30- 45 0.242 0.368 0.444 0.126 0.368   0.50   1.26   5.89   0.00   0.00   1.17
 45- 60 0.242 0.368 0.444 0.126 0.368   0.50   1.26   5.89   0.00   0.00   1.17
 60- 80 0.241 0.367 0.443 0.126 0.367   0.38   1.33   6.03   0.00   0.00   0.68
 80-100 0.241 0.367 0.443 0.126 0.367   0.38   1.33   6.03   0.00   0.00   0.68
100-150 0.228 0.351 0.433 0.123 0.351   0.05   1.38   6.21   0.00   0.00   0.39
150-200 0.228 0.351 0.433 0.123 0.351   0.05   1.38   6.21   0.00   0.00   0.39

TOT-200  46.3  71.1  87.3  24.9  71.1  <--cm   -  kg/ha-->    0.0    0.0 213044
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.     8     SAMPLE 8                

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  3 MAY    7 Emergence       16   0.00   2.0    1  4.4  0.00  0.01  0.00  0.00     1
 31 MAY   35 End Juveni     262   0.46  10.5   10  3.7  0.00  0.00  0.00  0.00     2
  5 JUN   40 Floral Ini     435   0.69  11.9   16  3.6  0.00  0.00  0.00  0.00     3
  5 AUG  101 75% Silkin    6214   2.82  24.6  101  1.6  0.02  0.09  0.00  0.00     4
 23 AUG  119 Beg Gr Fil   10778   2.46  24.6  107  1.0  0.00  0.15  0.00  0.00     5
 30 OCT  187 End Gr Fil   16925   0.59  24.6  115  0.7  0.25  0.41  0.00  0.00     6
  3 NOV  191 Maturity     16925   0.59  24.6  115  0.7  0.84  0.76  0.00  0.00    10
  3 NOV  191 Harvest      16925   0.59  24.6  115  0.7  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     101          -99
      Physiological maturity day (dap)                       191          -99
      Yield at harvest maturity (kg [dm]/ha)                6477          -99
      Number at maturity (no/m2)                            1770          -99
      Unit wt at maturity (g [dm]/unit)                   0.3659          -99
      Number at maturity (no/unit)                         442.6          -99
      Tops weight at maturity (kg [dm]/ha)                 16925          -99
      By-product produced (stalk) at maturity (kg[dm]/ha   10505          -99
      Leaf area index, maximum                              2.84          -99
      Harvest index at maturity                            0.383          -99
      Grain N at maturity (kg/ha)                             80          -99
      Tops N at maturity (kg/ha)                             115          -99
      Stem N at maturity (kg/ha)                              35          -99
      Grain N at maturity (%)                                1.2          -99
      Tops weight at anthesis (kg [dm]/ha)                  5898          -99
      Tops N at anthesis (kg/ha)                             101          -99
      Leaf number per stem at maturity                     24.62          -99
      Emergence day (dap)                                      7          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    28  23.0  13.5  18.2  20.1  12.02  380.0  173.3   74.7  134.8    0    0    0    0    0   16  0.000  0.000  0.002  0.006  0.000  0.000
 End Juvenil-Floral Init    5  24.0  12.2  18.1  22.3  12.02  380.0    0.0    9.0   25.5    0    0    0    0    0    0  0.000  0.000  0.001  0.002  0.000  0.000
 Floral Init-End Lf Grow   61  22.3  12.7  17.5  18.0  12.02  380.0  394.9  183.3  233.5    0    0    0    0    0   39  0.005  0.025  0.036  0.090  0.000  0.000
 End Lf Grth-Beg Grn Fil   18  22.2  12.6  17.4  20.4  12.02  380.0   48.5   75.7   76.1    0    0    0    0    0   14  0.000  0.000  0.061  0.153  0.000  0.000
 Grain Filling Phase       68  24.2  12.9  18.5  22.2  12.00  380.0  104.4  242.5  331.7    0    0    0    0    0   58  0.188  0.240  0.199  0.401  0.000  0.000
   
 Planting to Harvest      191  23.2  12.9  18.1  20.4  12.01  380.0  736.2  602.4  861.7    0    0    0    0    0  134  0.085  0.111  0.099  0.203  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 191 days 

 Precipitation during growing season       736.2 mm[rain]
   Dry Matter Productivity                  2.30 kg[DM]/m3[rain]          =   23.0 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.88 kg[grain yield]/m3[rain] =    8.8 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  602.4 mm[ET]
   Dry Matter Productivity                  2.81 kg[DM]/m3[ET]            =   28.1 kg[DM]/ha per mm[ET]
   Yield Productivity                       1.08 kg[grain yield]/m3[ET]   =   10.8 kg[yield]/ha per mm[ET]

 Transpiration during growing season       422.0 mm[EP]
   Dry Matter Productivity                  4.01 kg[DM]/m3[EP]            =   40.1 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.54 kg[grain yield]/m3[EP]   =   15.4 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                 112.8 kg[DM]/kg[N applied]
   Yield Productivity                       43.2 kg[yield]/kg[N applied]

 N uptake during growing season              186 kg[N uptake]/ha
   Dry Matter Productivity                  91.0 kg[DM]/kg[N uptake]
   Yield Productivity                       34.8 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     6477 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:38

*RUN   9        : SAMPLE 9                  MZCER048 EXPEFILE    9
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT  9   : SAMPLE 9                  MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEAI   2021
 SOIL           : IB00000008     TEXTURE : yLoam -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:245.9mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.210 0.333 0.426 0.123 0.333   1.00   1.19   5.85   0.00   0.00   2.55
  5- 15 0.222 0.346 0.432 0.124 0.346   0.85   1.20   5.93   0.00   0.00   2.16
 15- 30 0.226 0.350 0.433 0.124 0.350   0.70   1.22   5.88   0.00   0.00   1.58
 30- 45 0.239 0.364 0.441 0.125 0.364   0.50   1.28   5.99   0.00   0.00   1.01
 45- 60 0.239 0.364 0.441 0.125 0.364   0.50   1.28   5.99   0.00   0.00   1.01
 60- 80 0.240 0.363 0.440 0.123 0.363   0.38   1.34   6.14   0.00   0.00   0.59
 80-100 0.240 0.363 0.440 0.123 0.363   0.38   1.34   6.14   0.00   0.00   0.59
100-150 0.230 0.352 0.433 0.122 0.352   0.05   1.39   6.33   0.00   0.00   0.33
150-200 0.230 0.352 0.433 0.122 0.352   0.05   1.39   6.33   0.00   0.00   0.33

TOT-200  46.4  71.0  87.1  24.6  71.0  <--cm   -  kg/ha-->    0.0    0.0 186285
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.     9     SAMPLE 9                

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  2 MAY    6 Emergence       16   0.00   2.2    1  4.4  0.00  0.01  0.00  0.00     1
 21 MAY   25 End Juveni     244   0.43  10.3    9  3.5  0.00  0.00  0.00  0.00     2
 26 MAY   30 Floral Ini     512   0.78  12.2   18  3.6  0.00  0.00  0.00  0.00     3
  5 JUL   70 75% Silkin    5128   2.03  25.4   61  1.2  0.20  0.25  0.00  0.00     4
 18 JUL   83 Beg Gr Fil    6907   1.74  25.4   87  1.3  0.02  0.24  0.00  0.00     5
  7 SEP  134 End Gr Fil   10367   1.29  25.4   98  0.9  0.00  0.11  0.00  0.00     6
 10 SEP  137 Maturity     10367   1.29  25.4   98  0.9  0.00  0.21  0.00  0.00    10
 10 SEP  137 Harvest      10367   1.29  25.4   98  0.9  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                      70          -99
      Physiological maturity day (dap)                       137          -99
      Yield at harvest maturity (kg [dm]/ha)                3529          -99
      Number at maturity (no/m2)                            1153          -99
      Unit wt at maturity (g [dm]/unit)                   0.3060          -99
      Number at maturity (no/unit)                         288.4          -99
      Tops weight at maturity (kg [dm]/ha)                 10367          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    6892          -99
      Leaf area index, maximum                              2.51          -99
      Harvest index at maturity                            0.340          -99
      Grain N at maturity (kg/ha)                             55          -99
      Tops N at maturity (kg/ha)                              98          -99
      Stem N at maturity (kg/ha)                              43          -99
      Grain N at maturity (%)                                1.6          -99
      Tops weight at anthesis (kg [dm]/ha)                  4926          -99
      Tops N at anthesis (kg/ha)                              61          -99
      Leaf number per stem at maturity                     25.39          -99
      Emergence day (dap)                                      6          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    19  26.9  18.6  22.8  19.5  12.03  380.0  152.1   56.5   97.7    0    0    1    0    0   12  0.000  0.000  0.002  0.004  0.000  0.000
 End Juvenil-Floral Init    5  27.6  18.4  23.0  21.6  12.04  380.0   12.0   11.7   27.3    0    0    0    0    0    3  0.000  0.000  0.000  0.001  0.000  0.000
 Floral Init-End Lf Grow   40  28.3  17.8  23.1  21.2  12.04  380.0    3.6  135.6  200.0    0    0    0    0    0    9  0.109  0.197  0.097  0.241  0.000  0.000
 End Lf Grth-Beg Grn Fil   13  25.2  17.7  21.4  15.3  12.04  380.0  122.9   43.5   45.5    0    0    0    0    0   13  0.000  0.025  0.106  0.264  0.000  0.000
 Grain Filling Phase       51  26.5  17.5  22.0  19.4  12.03  380.0  454.5  218.1  232.4    0    0    0    0    0   39  0.000  0.000  0.043  0.108  0.000  0.000
   
 Planting to Harvest      137  27.2  17.8  22.5  19.8  12.03  380.0  752.9  487.0  655.8    0    0    4    0    0   79  0.032  0.060  0.056  0.141  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 137 days 

 Precipitation during growing season       752.9 mm[rain]
   Dry Matter Productivity                  1.38 kg[DM]/m3[rain]          =   13.8 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.47 kg[grain yield]/m3[rain] =    4.7 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  487.0 mm[ET]
   Dry Matter Productivity                  2.13 kg[DM]/m3[ET]            =   21.3 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.72 kg[grain yield]/m3[ET]   =    7.2 kg[yield]/ha per mm[ET]

 Transpiration during growing season       333.1 mm[EP]
   Dry Matter Productivity                  3.11 kg[DM]/m3[EP]            =   31.1 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.06 kg[grain yield]/m3[EP]   =   10.6 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                  69.1 kg[DM]/kg[N applied]
   Yield Productivity                       23.5 kg[yield]/kg[N applied]

 N uptake during growing season              147 kg[N uptake]/ha
   Dry Matter Productivity                  70.5 kg[DM]/kg[N uptake]
   Yield Productivity                       24.0 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     3529 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:38

*RUN  10        : SAMPLE 10                 MZCER048 EXPEFILE   10
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 10   : SAMPLE 10                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEAJ   2021
 SOIL           : IB00000003     TEXTURE : ClayL -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:245.1mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.203 0.326 0.422 0.123 0.326   1.00   1.22   6.12   0.00   0.00   2.12
  5- 15 0.219 0.343 0.431 0.124 0.343   0.85   1.24   6.20   0.00   0.00   1.79
 15- 30 0.219 0.342 0.430 0.123 0.342   0.70   1.25   6.17   0.00   0.00   1.26
 30- 45 0.237 0.362 0.440 0.125 0.362   0.50   1.30   6.29   0.00   0.00   0.80
 45- 60 0.237 0.362 0.440 0.125 0.362   0.50   1.30   6.29   0.00   0.00   0.80
 60- 80 0.236 0.360 0.438 0.124 0.360   0.38   1.36   6.41   0.00   0.00   0.47
 80-100 0.236 0.360 0.438 0.124 0.360   0.38   1.36   6.41   0.00   0.00   0.47
100-150 0.224 0.345 0.430 0.121 0.345   0.05   1.41   6.59   0.00   0.00   0.27
150-200 0.224 0.345 0.430 0.121 0.345   0.05   1.41   6.59   0.00   0.00   0.27

TOT-200  45.4  69.9  86.6  24.5  69.9  <--cm   -  kg/ha-->    0.0    0.0 153591
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    10     SAMPLE 10               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  3 MAY    7 Emergence       16   0.00   2.0    1  4.4  0.00  0.01  0.00  0.00     1
 28 MAY   32 End Juveni     245   0.43  10.3    9  3.7  0.00  0.00  0.00  0.00     2
  2 JUN   37 Floral Ini     448   0.70  11.9   16  3.6  0.00  0.00  0.00  0.00     3
 26 JUL   91 75% Silkin    4820   1.70  24.9   65  1.3  0.10  0.25  0.00  0.00     4
 12 AUG  108 Beg Gr Fil    7309   1.21  24.9   68  0.9  0.00  0.39  0.00  0.00     5
 14 OCT  171 End Gr Fil   10743   0.38  24.9   81  0.8  0.00  0.38  0.00  0.00     6
 18 OCT  175 Maturity     10743   0.38  24.9   81  0.8  0.00  0.43  0.00  0.00    10
 18 OCT  175 Harvest      10743   0.38  24.9   81  0.8  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                      91          -99
      Physiological maturity day (dap)                       175          -99
      Yield at harvest maturity (kg [dm]/ha)                3502          -99
      Number at maturity (no/m2)                             923          -99
      Unit wt at maturity (g [dm]/unit)                   0.3794          -99
      Number at maturity (no/unit)                         230.8          -99
      Tops weight at maturity (kg [dm]/ha)                 10743          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    7292          -99
      Leaf area index, maximum                              1.99          -99
      Harvest index at maturity                            0.326          -99
      Grain N at maturity (kg/ha)                             42          -99
      Tops N at maturity (kg/ha)                              81          -99
      Stem N at maturity (kg/ha)                              39          -99
      Grain N at maturity (%)                                1.2          -99
      Tops weight at anthesis (kg [dm]/ha)                  4757          -99
      Tops N at anthesis (kg/ha)                              65          -99
      Leaf number per stem at maturity                     24.89          -99
      Emergence day (dap)                                      7          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    25  23.4  15.1  19.2  20.5  12.05  380.0  216.9   63.2  125.2    0    0    0    0    0   15  0.000  0.000  0.002  0.005  0.000  0.000
 End Juvenil-Floral Init    5  25.3  14.6  20.0  23.0  12.06  380.0    1.4   11.3   27.4    0    0    0    0    0    2  0.000  0.000  0.001  0.001  0.000  0.000
 Floral Init-End Lf Grow   54  23.5  14.4  18.9  18.7  12.06  380.0  470.4  163.0  223.2    0    0    0    0    0   31  0.061  0.103  0.096  0.239  0.000  0.000
 End Lf Grth-Beg Grn Fil   17  21.9  14.3  18.1  18.3  12.04  380.0  144.0   66.3   67.4    0    0    0    0    0   13  0.000  0.000  0.157  0.393  0.000  0.000
 Grain Filling Phase       63  24.0  14.4  19.2  22.0  12.01  380.0  350.0  263.9  321.7    0    0    0    0    0   53  0.000  0.000  0.150  0.375  0.000  0.000
   
 Planting to Harvest      175  23.7  14.5  19.1  20.5  12.03  380.0   1184    584    829    0    0    0    0    0  117  0.019  0.032  0.103  0.257  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 175 days 

 Precipitation during growing season      1183.7 mm[rain]
   Dry Matter Productivity                  0.91 kg[DM]/m3[rain]          =    9.1 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.30 kg[grain yield]/m3[rain] =    3.0 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  584.1 mm[ET]
   Dry Matter Productivity                  1.84 kg[DM]/m3[ET]            =   18.4 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.60 kg[grain yield]/m3[ET]   =    6.0 kg[yield]/ha per mm[ET]

 Transpiration during growing season       323.2 mm[EP]
   Dry Matter Productivity                  3.32 kg[DM]/m3[EP]            =   33.2 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.08 kg[grain yield]/m3[EP]   =   10.8 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                  71.6 kg[DM]/kg[N applied]
   Yield Productivity                       23.3 kg[yield]/kg[N applied]

 N uptake during growing season              130 kg[N uptake]/ha
   Dry Matter Productivity                  82.6 kg[DM]/kg[N uptake]
   Yield Productivity                       26.9 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     3502 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:38

*RUN  11        : SAMPLE 11                 MZCER048 EXPEFILE   11
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 11   : SAMPLE 11                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEAK   2021
 SOIL           : IB00000001     TEXTURE : yLoam -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:252.5mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.211 0.339 0.430 0.128 0.339   1.00   1.21   6.19   0.00   0.00   1.75
  5- 15 0.224 0.351 0.436 0.127 0.351   0.85   1.23   6.26   0.00   0.00   1.49
 15- 30 0.226 0.354 0.438 0.128 0.354   0.70   1.24   6.27   0.00   0.00   1.08
 30- 45 0.241 0.369 0.446 0.128 0.369   0.50   1.29   6.38   0.00   0.00   0.70
 45- 60 0.241 0.369 0.446 0.128 0.369   0.50   1.29   6.38   0.00   0.00   0.70
 60- 80 0.241 0.368 0.444 0.127 0.368   0.38   1.35   6.52   0.00   0.00   0.40
 80-100 0.241 0.368 0.444 0.127 0.368   0.38   1.35   6.52   0.00   0.00   0.40
100-150 0.231 0.356 0.437 0.125 0.356   0.05   1.41   6.69   0.00   0.00   0.23
150-200 0.231 0.356 0.437 0.125 0.356   0.05   1.41   6.69   0.00   0.00   0.23

TOT-200  46.7  71.9  87.9  25.3  71.9  <--cm   -  kg/ha-->    0.0    0.0 130123
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    11     SAMPLE 11               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 27 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 28 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  7 MAY   10 Emergence       16   0.00   1.8    1  4.4  0.00  0.00  0.00  0.00     1
 15 JUN   49 End Juveni     222   0.40  10.1    9  3.9  0.00  0.01  0.00  0.00     2
 20 JUN   54 Floral Ini     315   0.53  11.0   11  3.6  0.00  0.00  0.00  0.00     3
 18 SEP  144 75% Silkin    3730   0.86  23.0   38  1.0  0.00  0.28  0.00  0.00     4
 11 OCT  167 Beg Gr Fil    4933   0.41  23.0   37  0.8  0.00  0.62  0.00  0.00     5
 11 JAN  259 End Gr Fil    6437   0.03  23.0   46  0.7  0.00  0.54  0.00  0.00     6
 16 JAN  264 Maturity      6437   0.03  23.0   46  0.7  0.00  0.34  0.00  0.00    10
 16 JAN  264 Harvest       6437   0.03  23.0   46  0.7  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     144          -99
      Physiological maturity day (dap)                       264          -99
      Yield at harvest maturity (kg [dm]/ha)                1911          -99
      Number at maturity (no/m2)                             354          -99
      Unit wt at maturity (g [dm]/unit)                   0.5405          -99
      Number at maturity (no/unit)                          88.4          -99
      Tops weight at maturity (kg [dm]/ha)                  6437          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    4568          -99
      Leaf area index, maximum                              1.68          -99
      Harvest index at maturity                            0.297          -99
      Grain N at maturity (kg/ha)                             19          -99
      Tops N at maturity (kg/ha)                              46          -99
      Stem N at maturity (kg/ha)                              27          -99
      Grain N at maturity (%)                                1.0          -99
      Tops weight at anthesis (kg [dm]/ha)                  3698          -99
      Tops N at anthesis (kg/ha)                              38          -99
      Leaf number per stem at maturity                     23.01          -99
      Emergence day (dap)                                     10          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    39  19.6  10.1  14.9  19.4  12.06  380.0  518.4  106.3  169.4    0    0    0    0    0   34  0.000  0.000  0.002  0.006  0.000  0.000
 End Juvenil-Floral Init    5  18.4  10.4  14.4  17.0  12.06  380.0   87.6   18.2   18.2    0    0    0    0    0    5  0.000  0.000  0.000  0.000  0.000  0.000
 Floral Init-End Lf Grow   90  18.4   9.9  14.2  17.7  12.04  380.0   3431    317    322    0    0    0    0    0   88  0.000  0.000  0.124  0.273  0.000  0.000
 End Lf Grth-Beg Grn Fil   23  19.9  10.4  15.2  19.4  11.99  380.0   1211     96     97    0    0    0    0    0   23  0.000  0.000  0.347  0.622  0.000  0.000
 Grain Filling Phase       92  21.4  10.1  15.8  21.3  11.95  380.0  506.7  255.0  446.1    0    0    0    0    0   70  0.000  0.000  0.272  0.547  0.000  0.000
   
 Planting to Harvest      264  20.0  10.1  15.0  19.6  12.01  380.0   6268    829   1129    0    0    0    0    0  231  0.000  0.000  0.170  0.345  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 264 days 

 Precipitation during growing season      6267.6 mm[rain]
   Dry Matter Productivity                  0.10 kg[DM]/m3[rain]          =    1.0 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.03 kg[grain yield]/m3[rain] =    0.3 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  829.0 mm[ET]
   Dry Matter Productivity                  0.78 kg[DM]/m3[ET]            =    7.8 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.23 kg[grain yield]/m3[ET]   =    2.3 kg[yield]/ha per mm[ET]

 Transpiration during growing season       270.7 mm[EP]
   Dry Matter Productivity                  2.38 kg[DM]/m3[EP]            =   23.8 kg[DM]/ha per mm[EP]
   Yield Productivity                       0.71 kg[grain yield]/m3[EP]   =    7.1 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                  42.9 kg[DM]/kg[N applied]
   Yield Productivity                       12.7 kg[yield]/kg[N applied]

 N uptake during growing season               99 kg[N uptake]/ha
   Dry Matter Productivity                  65.0 kg[DM]/kg[N uptake]
   Yield Productivity                       19.3 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     1911 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:38

*RUN  12        : SAMPLE 12                 MZCER048 EXPEFILE   12
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 12   : SAMPLE 12                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEAL   2021
 SOIL           : IB00000009     TEXTURE : yLoam -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:253.8mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.217 0.343 0.432 0.126 0.343   1.00   1.14   6.01   0.00   0.00   2.35
  5- 15 0.229 0.356 0.438 0.127 0.356   0.85   1.17   6.08   0.00   0.00   1.99
 15- 30 0.233 0.361 0.441 0.128 0.361   0.70   1.18   6.07   0.00   0.00   1.44
 30- 45 0.246 0.374 0.449 0.128 0.374   0.50   1.23   6.18   0.00   0.00   0.92
 45- 60 0.246 0.374 0.449 0.128 0.374   0.50   1.23   6.18   0.00   0.00   0.92
 60- 80 0.246 0.374 0.448 0.128 0.374   0.38   1.31   6.31   0.00   0.00   0.54
 80-100 0.246 0.374 0.448 0.128 0.374   0.38   1.31   6.31   0.00   0.00   0.54
100-150 0.237 0.363 0.441 0.126 0.363   0.05   1.36   6.50   0.00   0.00   0.31
150-200 0.237 0.363 0.441 0.126 0.363   0.05   1.36   6.50   0.00   0.00   0.31

TOT-200  47.8  73.2  88.6  25.4  73.2  <--cm   -  kg/ha-->    0.0    0.0 166570
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    12     SAMPLE 12               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 30 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
  1 MAY    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  9 MAY    9 Emergence       16   0.00   1.8    1  4.4  0.00  0.00  0.00  0.00     1
  9 JUN   40 End Juveni     240   0.43  10.3    9  3.8  0.00  0.01  0.00  0.00     2
 14 JUN   45 Floral Ini     383   0.62  11.5   14  3.6  0.00  0.00  0.00  0.00     3
 25 AUG  117 75% Silkin    6687   3.41  23.9  109  1.6  0.00  0.02  0.00  0.00     4
 14 SEP  137 Beg Gr Fil   11775   2.95  23.9  111  0.9  0.00  0.15  0.00  0.00     5
  3 DEC  217 End Gr Fil   19550   0.38  23.9  123  0.6  0.00  0.51  0.00  0.00     6
  8 DEC  222 Maturity     19550   0.38  23.9  123  0.6  0.00  0.80  0.00  0.00    10
  8 DEC  222 Harvest      19550   0.38  23.9  123  0.6  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     117          -99
      Physiological maturity day (dap)                       222          -99
      Yield at harvest maturity (kg [dm]/ha)                8561          -99
      Number at maturity (no/m2)                            1785          -99
      Unit wt at maturity (g [dm]/unit)                   0.4795          -99
      Number at maturity (no/unit)                         446.3          -99
      Tops weight at maturity (kg [dm]/ha)                 19550          -99
      By-product produced (stalk) at maturity (kg[dm]/ha   11047          -99
      Leaf area index, maximum                              3.43          -99
      Harvest index at maturity                            0.438          -99
      Grain N at maturity (kg/ha)                             89          -99
      Tops N at maturity (kg/ha)                             123          -99
      Stem N at maturity (kg/ha)                              33          -99
      Grain N at maturity (%)                                1.0          -99
      Tops weight at anthesis (kg [dm]/ha)                  6353          -99
      Tops N at anthesis (kg/ha)                             109          -99
      Leaf number per stem at maturity                     23.93          -99
      Emergence day (dap)                                      9          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    31  22.0  11.8  16.9  19.7  12.06  380.0  148.9  107.0  143.1    0    0    0    0    0   30  0.000  0.000  0.003  0.007  0.000  0.000
 End Juvenil-Floral Init    5  22.3  11.5  16.9  20.9  12.06  380.0    3.8   10.9   23.5    0    0    0    0    0    5  0.000  0.000  0.000  0.001  0.000  0.000
 Floral Init-End Lf Grow   72  20.4  11.5  15.9  18.1  12.05  380.0  548.8  259.2  267.0    0    0    0    0    0   72  0.000  0.000  0.006  0.015  0.000  0.000
 End Lf Grth-Beg Grn Fil   20  20.8  11.8  16.3  19.2  12.02  380.0  309.1   76.6   77.0    0    0    0    0    0   19  0.000  0.000  0.058  0.144  0.000  0.000
 Grain Filling Phase       80  22.3  11.6  17.0  20.8  11.97  380.0  936.0  339.7  356.2    0    0    0    0    0   80  0.000  0.000  0.274  0.504  0.000  0.000
   
 Planting to Harvest      222  21.5  11.6  16.6  19.6  12.02  380.0   2211    843    932    0    0    0    0    0  219  0.000  0.000  0.119  0.218  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 222 days 

 Precipitation during growing season      2210.9 mm[rain]
   Dry Matter Productivity                  0.88 kg[DM]/m3[rain]          =    8.8 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.39 kg[grain yield]/m3[rain] =    3.9 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  843.1 mm[ET]
   Dry Matter Productivity                  2.32 kg[DM]/m3[ET]            =   23.2 kg[DM]/ha per mm[ET]
   Yield Productivity                       1.02 kg[grain yield]/m3[ET]   =   10.2 kg[yield]/ha per mm[ET]

 Transpiration during growing season       496.0 mm[EP]
   Dry Matter Productivity                  3.94 kg[DM]/m3[EP]            =   39.4 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.73 kg[grain yield]/m3[EP]   =   17.3 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                 130.3 kg[DM]/kg[N applied]
   Yield Productivity                       57.1 kg[yield]/kg[N applied]

 N uptake during growing season              212 kg[N uptake]/ha
   Dry Matter Productivity                  92.2 kg[DM]/kg[N uptake]
   Yield Productivity                       40.4 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     8561 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:38

*RUN  13        : SAMPLE 13                 MZCER048 EXPEFILE   13
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 13   : SAMPLE 13                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEAM   2021
 SOIL           : IB00000010     TEXTURE : ClayL -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:254.5mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.231 0.358 0.439 0.127 0.358   1.00   1.15   5.60   0.00   0.00   3.03
  5- 15 0.248 0.375 0.449 0.127 0.375   0.85   1.16   5.66   0.00   0.00   2.57
 15- 30 0.252 0.380 0.452 0.128 0.380   0.70   1.18   5.63   0.00   0.00   1.95
 30- 45 0.272 0.400 0.465 0.128 0.400   0.50   1.24   5.75   0.00   0.00   1.25
 45- 60 0.272 0.400 0.465 0.128 0.400   0.50   1.24   5.75   0.00   0.00   1.25
 60- 80 0.271 0.398 0.463 0.127 0.398   0.38   1.31   5.88   0.00   0.00   0.73
 80-100 0.271 0.398 0.463 0.127 0.398   0.38   1.31   5.88   0.00   0.00   0.73
100-150 0.257 0.384 0.453 0.127 0.384   0.05   1.36   6.06   0.00   0.00   0.42
150-200 0.257 0.384 0.453 0.127 0.384   0.05   1.36   6.06   0.00   0.00   0.42

TOT-200  52.1  77.6  91.2  25.4  77.6  <--cm   -  kg/ha-->    0.0    0.0 223622
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    13     SAMPLE 13               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  4 MAY    8 Emergence       16   0.00   1.9    1  4.4  0.00  0.00  0.00  0.00     1
  6 JUN   41 End Juveni     247   0.44  10.4    9  3.8  0.00  0.01  0.00  0.00     2
 11 JUN   46 Floral Ini     388   0.63  11.5   14  3.6  0.00  0.00  0.00  0.00     3
 24 AUG  120 75% Silkin    6428   3.35  23.9  109  1.7  0.01  0.01  0.00  0.00     4
 14 SEP  141 Beg Gr Fil   12675   3.12  23.9  116  0.9  0.00  0.07  0.00  0.00     5
 29 NOV  217 End Gr Fil   18280   0.39  23.9  114  0.6  0.54  0.44  0.00  0.00     6
  4 DEC  222 Maturity     18280   0.39  23.9  114  0.6  0.79  0.83  0.00  0.00    10
  4 DEC  222 Harvest      18280   0.39  23.9  114  0.6  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     120          -99
      Physiological maturity day (dap)                       222          -99
      Yield at harvest maturity (kg [dm]/ha)                6809          -99
      Number at maturity (no/m2)                            2000          -99
      Unit wt at maturity (g [dm]/unit)                   0.3404          -99
      Number at maturity (no/unit)                         500.0          -99
      Tops weight at maturity (kg [dm]/ha)                 18280          -99
      By-product produced (stalk) at maturity (kg[dm]/ha   11529          -99
      Leaf area index, maximum                              3.41          -99
      Harvest index at maturity                            0.372          -99
      Grain N at maturity (kg/ha)                             86          -99
      Tops N at maturity (kg/ha)                             114          -99
      Stem N at maturity (kg/ha)                              28          -99
      Grain N at maturity (%)                                1.3          -99
      Tops weight at anthesis (kg [dm]/ha)                  6043          -99
      Tops N at anthesis (kg/ha)                             109          -99
      Leaf number per stem at maturity                     23.92          -99
      Emergence day (dap)                                      8          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    33  20.9  12.2  16.5  20.3  12.02  380.0  166.7   76.2  154.4    0    0    0    0    0   15  0.000  0.000  0.002  0.006  0.000  0.000
 End Juvenil-Floral Init    5  21.9  11.1  16.5  22.7  12.02  380.0    0.0    7.7   25.2    0    0    0    0    0    0  0.000  0.000  0.000  0.001  0.000  0.000
 Floral Init-End Lf Grow   74  20.0  11.3  15.7  17.9  12.02  380.0  443.5  224.7  269.5    0    0    0    0    0   54  0.000  0.007  0.006  0.015  0.000  0.000
 End Lf Grth-Beg Grn Fil   21  21.1  11.3  16.2  21.1  12.01  380.0   52.2   88.4   88.9    0    0    0    0    0   15  0.000  0.000  0.028  0.070  0.000  0.000
 Grain Filling Phase       76  22.8  11.9  17.4  22.6  11.99  380.0   58.5  174.2  367.2    0    0    0    0    0   55  0.459  0.525  0.227  0.432  0.000  0.000
   
 Planting to Harvest      222  21.4  11.7  16.6  20.5  12.01  380.0  747.6  595.6  970.2    0    0    0    0    0  147  0.173  0.200  0.096  0.179  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 222 days 

 Precipitation during growing season       747.6 mm[rain]
   Dry Matter Productivity                  2.45 kg[DM]/m3[rain]          =   24.5 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.91 kg[grain yield]/m3[rain] =    9.1 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  595.6 mm[ET]
   Dry Matter Productivity                  3.07 kg[DM]/m3[ET]            =   30.7 kg[DM]/ha per mm[ET]
   Yield Productivity                       1.14 kg[grain yield]/m3[ET]   =   11.4 kg[yield]/ha per mm[ET]

 Transpiration during growing season       432.1 mm[EP]
   Dry Matter Productivity                  4.23 kg[DM]/m3[EP]            =   42.3 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.58 kg[grain yield]/m3[EP]   =   15.8 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                 121.9 kg[DM]/kg[N applied]
   Yield Productivity                       45.4 kg[yield]/kg[N applied]

 N uptake during growing season              216 kg[N uptake]/ha
   Dry Matter Productivity                  84.6 kg[DM]/kg[N uptake]
   Yield Productivity                       31.5 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     6809 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:38

*RUN  14        : SAMPLE 14                 MZCER048 EXPEFILE   14
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 14   : SAMPLE 14                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEAN   2021
 SOIL           : IB00000011     TEXTURE : yLoam -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:247.6mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.217 0.342 0.430 0.125 0.342   1.00   1.20   5.99   0.00   0.00   2.16
  5- 15 0.229 0.354 0.436 0.125 0.354   0.85   1.22   6.06   0.00   0.00   1.83
 15- 30 0.231 0.356 0.437 0.125 0.356   0.70   1.23   6.02   0.00   0.00   1.32
 30- 45 0.243 0.368 0.443 0.125 0.368   0.50   1.28   6.15   0.00   0.00   0.85
 45- 60 0.243 0.368 0.443 0.125 0.368   0.50   1.28   6.15   0.00   0.00   0.85
 60- 80 0.244 0.368 0.443 0.124 0.368   0.38   1.35   6.31   0.00   0.00   0.49
 80-100 0.244 0.368 0.443 0.124 0.368   0.38   1.35   6.31   0.00   0.00   0.49
100-150 0.235 0.358 0.436 0.123 0.358   0.05   1.41   6.51   0.00   0.00   0.28
150-200 0.235 0.358 0.436 0.123 0.358   0.05   1.41   6.51   0.00   0.00   0.28

TOT-200  47.4  72.2  87.7  24.8  72.2  <--cm   -  kg/ha-->    0.0    0.0 158220
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    14     SAMPLE 14               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  2 MAY    6 Emergence       16   0.00   2.1    1  4.4  0.00  0.01  0.00  0.00     1
 26 MAY   30 End Juveni     274   0.48  10.6   10  3.6  0.00  0.00  0.00  0.00     2
 31 MAY   35 Floral Ini     517   0.79  12.3   19  3.6  0.00  0.00  0.00  0.00     3
 20 JUL   85 75% Silkin    4858   1.73  25.4   72  1.5  0.18  0.25  0.00  0.00     4
  5 AUG  101 Beg Gr Fil    7024   1.41  25.4   73  1.0  0.00  0.25  0.00  0.00     5
  3 OCT  160 End Gr Fil   10869   0.61  25.4   86  0.8  0.00  0.29  0.00  0.00     6
  7 OCT  164 Maturity     10869   0.61  25.4   86  0.8  0.00  0.48  0.00  0.00    10
  7 OCT  164 Harvest      10869   0.61  25.4   86  0.8  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                      85          -99
      Physiological maturity day (dap)                       164          -99
      Yield at harvest maturity (kg [dm]/ha)                3912          -99
      Number at maturity (no/m2)                            1105          -99
      Unit wt at maturity (g [dm]/unit)                   0.3541          -99
      Number at maturity (no/unit)                         276.2          -99
      Tops weight at maturity (kg [dm]/ha)                 10869          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    7009          -99
      Leaf area index, maximum                              2.08          -99
      Harvest index at maturity                            0.360          -99
      Grain N at maturity (kg/ha)                             52          -99
      Tops N at maturity (kg/ha)                              86          -99
      Stem N at maturity (kg/ha)                              34          -99
      Grain N at maturity (%)                                1.3          -99
      Tops weight at anthesis (kg [dm]/ha)                  4688          -99
      Tops N at anthesis (kg/ha)                              72          -99
      Leaf number per stem at maturity                     25.42          -99
      Emergence day (dap)                                      6          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    24  24.6  15.9  20.2  20.1  12.04  380.0  176.1   64.5  120.6    0    0    0    0    0   15  0.000  0.000  0.002  0.005  0.000  0.000
 End Juvenil-Floral Init    5  25.7  15.6  20.7  22.5  12.05  380.0    0.2   11.7   27.1    0    0    0    0    0    1  0.000  0.000  0.001  0.002  0.000  0.000
 Floral Init-End Lf Grow   50  25.1  15.1  20.1  19.1  12.05  380.0  203.0  142.0  215.1    0    0    0    0    0   23  0.133  0.183  0.102  0.249  0.000  0.000
 End Lf Grth-Beg Grn Fil   16  22.3  14.8  18.6  16.0  12.04  380.0  247.8   55.5   56.2    0    0    0    0    0   14  0.000  0.000  0.102  0.256  0.000  0.000
 Grain Filling Phase       59  25.2  14.8  20.0  21.7  12.02  380.0  186.5  242.7  296.2    0    0    0    0    0   42  0.000  0.000  0.114  0.285  0.000  0.000
   
 Planting to Harvest      164  24.9  15.1  20.0  20.2  12.03  380.0  818.4  537.9  773.1    0    0    0    0    0  100  0.041  0.056  0.087  0.216  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 164 days 

 Precipitation during growing season       818.4 mm[rain]
   Dry Matter Productivity                  1.33 kg[DM]/m3[rain]          =   13.3 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.48 kg[grain yield]/m3[rain] =    4.8 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  537.9 mm[ET]
   Dry Matter Productivity                  2.02 kg[DM]/m3[ET]            =   20.2 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.73 kg[grain yield]/m3[ET]   =    7.3 kg[yield]/ha per mm[ET]

 Transpiration during growing season       330.0 mm[EP]
   Dry Matter Productivity                  3.29 kg[DM]/m3[EP]            =   32.9 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.19 kg[grain yield]/m3[EP]   =   11.9 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                  72.5 kg[DM]/kg[N applied]
   Yield Productivity                       26.1 kg[yield]/kg[N applied]

 N uptake during growing season              134 kg[N uptake]/ha
   Dry Matter Productivity                  81.1 kg[DM]/kg[N uptake]
   Yield Productivity                       29.2 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     3912 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:38

*RUN  15        : SAMPLE 15                 MZCER048 EXPEFILE   15
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 15   : SAMPLE 15                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEAO   2021
 SOIL           : IB00000005     TEXTURE : Loam  -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:248.2mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.199 0.322 0.421 0.123 0.322   1.00   1.16   5.73   0.00   0.00   3.14
  5- 15 0.217 0.342 0.430 0.125 0.342   0.85   1.18   5.80   0.00   0.00   2.65
 15- 30 0.219 0.344 0.431 0.125 0.344   0.70   1.19   5.76   0.00   0.00   2.00
 30- 45 0.240 0.366 0.443 0.126 0.366   0.50   1.24   5.87   0.00   0.00   1.28
 45- 60 0.240 0.366 0.443 0.126 0.366   0.50   1.24   5.87   0.00   0.00   1.28
 60- 80 0.239 0.364 0.441 0.125 0.364   0.38   1.33   6.00   0.00   0.00   0.75
 80-100 0.239 0.364 0.441 0.125 0.364   0.38   1.33   6.00   0.00   0.00   0.75
100-150 0.224 0.347 0.431 0.123 0.347   0.05   1.39   6.18   0.00   0.00   0.42
150-200 0.224 0.347 0.431 0.123 0.347   0.05   1.39   6.18   0.00   0.00   0.42

TOT-200  45.6  70.4  86.9  24.8  70.4  <--cm   -  kg/ha-->    0.0    0.0 231078
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    15     SAMPLE 15               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  4 MAY    8 Emergence       16   0.00   1.9    1  4.4  0.00  0.00  0.00  0.00     1
  1 JUN   36 End Juveni     223   0.40  10.0    8  3.7  0.00  0.00  0.00  0.00     2
  6 JUN   41 Floral Ini     387   0.62  11.5   14  3.6  0.00  0.00  0.00  0.00     3
  9 AUG  105 75% Silkin    6546   3.40  24.1  117  1.8  0.00  0.03  0.00  0.00     4
 27 AUG  123 Beg Gr Fil   12240   3.23  24.1  122  1.0  0.00  0.03  0.00  0.00     5
  8 NOV  196 End Gr Fil   20821   0.85  24.1  142  0.7  0.00  0.37  0.00  0.00     6
 12 NOV  200 Maturity     20821   0.85  24.1  142  0.7  0.00  0.79  0.00  0.00    10
 12 NOV  200 Harvest      20821   0.85  24.1  142  0.7  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     105          -99
      Physiological maturity day (dap)                       200          -99
      Yield at harvest maturity (kg [dm]/ha)                8747          -99
      Number at maturity (no/m2)                            2000          -99
      Unit wt at maturity (g [dm]/unit)                   0.4374          -99
      Number at maturity (no/unit)                         500.0          -99
      Tops weight at maturity (kg [dm]/ha)                 20821          -99
      By-product produced (stalk) at maturity (kg[dm]/ha   12132          -99
      Leaf area index, maximum                              3.41          -99
      Harvest index at maturity                            0.420          -99
      Grain N at maturity (kg/ha)                            108          -99
      Tops N at maturity (kg/ha)                             142          -99
      Stem N at maturity (kg/ha)                              34          -99
      Grain N at maturity (%)                                1.2          -99
      Tops weight at anthesis (kg [dm]/ha)                  6251          -99
      Tops N at anthesis (kg/ha)                             116          -99
      Leaf number per stem at maturity                     24.12          -99
      Emergence day (dap)                                      8          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    28  21.8  13.6  17.7  20.9  12.05  380.0  182.4   65.9  138.5    0    0    0    0    0   16  0.000  0.000  0.002  0.005  0.000  0.000
 End Juvenil-Floral Init    5  23.4  13.3  18.3  21.9  12.06  380.0    0.1    8.6   25.2    0    0    0    0    0    1  0.000  0.000  0.000  0.001  0.000  0.000
 Floral Init-End Lf Grow   64  21.2  12.9  17.1  18.4  12.06  380.0  582.2  196.8  246.7    0    0    0    0    0   40  0.000  0.005  0.011  0.027  0.000  0.000
 End Lf Grth-Beg Grn Fil   18  21.6  12.7  17.2  21.6  12.03  380.0   65.6   78.8   79.2    0    0    0    0    0   13  0.000  0.000  0.012  0.029  0.000  0.000
 Grain Filling Phase       73  22.6  12.9  17.8  22.5  11.99  380.0  316.7  307.8  347.4    0    0    0    0    0   45  0.000  0.000  0.184  0.360  0.000  0.000
   
 Planting to Harvest      200  22.1  13.1  17.6  20.9  12.03  380.0   1184    673    903    0    0    0    0    0  120  0.000  0.002  0.083  0.159  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 200 days 

 Precipitation during growing season      1184.1 mm[rain]
   Dry Matter Productivity                  1.76 kg[DM]/m3[rain]          =   17.6 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.74 kg[grain yield]/m3[rain] =    7.4 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  672.6 mm[ET]
   Dry Matter Productivity                  3.10 kg[DM]/m3[ET]            =   31.0 kg[DM]/ha per mm[ET]
   Yield Productivity                       1.30 kg[grain yield]/m3[ET]   =   13.0 kg[yield]/ha per mm[ET]

 Transpiration during growing season       531.5 mm[EP]
   Dry Matter Productivity                  3.92 kg[DM]/m3[EP]            =   39.2 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.65 kg[grain yield]/m3[EP]   =   16.5 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                 138.8 kg[DM]/kg[N applied]
   Yield Productivity                       58.3 kg[yield]/kg[N applied]

 N uptake during growing season              235 kg[N uptake]/ha
   Dry Matter Productivity                  88.6 kg[DM]/kg[N uptake]
   Yield Productivity                       37.2 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     8747 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:38

*RUN  16        : SAMPLE 16                 MZCER048 EXPEFILE   16
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 16   : SAMPLE 16                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEAP   2021
 SOIL           : IB00000005     TEXTURE : Loam  -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:248.2mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.199 0.322 0.421 0.123 0.322   1.00   1.16   5.73   0.00   0.00   3.14
  5- 15 0.217 0.342 0.430 0.125 0.342   0.85   1.18   5.80   0.00   0.00   2.65
 15- 30 0.219 0.344 0.431 0.125 0.344   0.70   1.19   5.76   0.00   0.00   2.00
 30- 45 0.240 0.366 0.443 0.126 0.366   0.50   1.24   5.87   0.00   0.00   1.28
 45- 60 0.240 0.366 0.443 0.126 0.366   0.50   1.24   5.87   0.00   0.00   1.28
 60- 80 0.239 0.364 0.441 0.125 0.364   0.38   1.33   6.00   0.00   0.00   0.75
 80-100 0.239 0.364 0.441 0.125 0.364   0.38   1.33   6.00   0.00   0.00   0.75
100-150 0.224 0.347 0.431 0.123 0.347   0.05   1.39   6.18   0.00   0.00   0.42
150-200 0.224 0.347 0.431 0.123 0.347   0.05   1.39   6.18   0.00   0.00   0.42

TOT-200  45.6  70.4  86.9  24.8  70.4  <--cm   -  kg/ha-->    0.0    0.0 231078
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    16     SAMPLE 16               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  3 MAY    7 Emergence       16   0.00   2.0    1  4.4  0.00  0.01  0.00  0.00     1
 28 MAY   32 End Juveni     245   0.43  10.3    9  3.7  0.00  0.00  0.00  0.00     2
  2 JUN   37 Floral Ini     448   0.70  11.9   16  3.6  0.00  0.00  0.00  0.00     3
 26 JUL   91 75% Silkin    5250   2.07  24.9   78  1.5  0.10  0.18  0.00  0.00     4
 12 AUG  108 Beg Gr Fil    8355   1.63  24.9   84  1.0  0.00  0.27  0.00  0.00     5
 14 OCT  171 End Gr Fil   13094   0.62  24.9  103  0.8  0.00  0.31  0.00  0.00     6
 18 OCT  175 Maturity     13094   0.62  24.9  103  0.8  0.00  0.46  0.00  0.00    10
 18 OCT  175 Harvest      13094   0.62  24.9  103  0.8  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                      91          -99
      Physiological maturity day (dap)                       175          -99
      Yield at harvest maturity (kg [dm]/ha)                4819          -99
      Number at maturity (no/m2)                            1274          -99
      Unit wt at maturity (g [dm]/unit)                   0.3783          -99
      Number at maturity (no/unit)                         318.5          -99
      Tops weight at maturity (kg [dm]/ha)                 13094          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    8328          -99
      Leaf area index, maximum                              2.17          -99
      Harvest index at maturity                            0.368          -99
      Grain N at maturity (kg/ha)                             63          -99
      Tops N at maturity (kg/ha)                             103          -99
      Stem N at maturity (kg/ha)                              40          -99
      Grain N at maturity (%)                                1.3          -99
      Tops weight at anthesis (kg [dm]/ha)                  5175          -99
      Tops N at anthesis (kg/ha)                              78          -99
      Leaf number per stem at maturity                     24.89          -99
      Emergence day (dap)                                      7          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    25  23.4  15.1  19.2  20.5  12.05  380.0  216.9   63.2  125.2    0    0    0    0    0   15  0.000  0.000  0.002  0.005  0.000  0.000
 End Juvenil-Floral Init    5  25.3  14.6  20.0  23.0  12.06  380.0    1.4   11.3   27.4    0    0    0    0    0    2  0.000  0.000  0.001  0.001  0.000  0.000
 Floral Init-End Lf Grow   54  23.5  14.4  18.9  18.7  12.06  380.0  470.4  164.5  222.4    0    0    0    0    0   31  0.055  0.096  0.069  0.172  0.000  0.000
 End Lf Grth-Beg Grn Fil   17  21.9  14.3  18.1  18.3  12.04  380.0  144.0   65.0   66.5    0    0    0    0    0   13  0.000  0.000  0.110  0.275  0.000  0.000
 Grain Filling Phase       63  24.0  14.4  19.2  22.0  12.01  380.0  350.0  283.8  314.2    0    0    0    0    0   53  0.000  0.000  0.123  0.308  0.000  0.000
   
 Planting to Harvest      175  23.7  14.5  19.1  20.5  12.03  380.0   1184    607    820    0    0    0    0    0  117  0.017  0.030  0.081  0.202  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 175 days 

 Precipitation during growing season      1183.7 mm[rain]
   Dry Matter Productivity                  1.11 kg[DM]/m3[rain]          =   11.1 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.41 kg[grain yield]/m3[rain] =    4.1 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  607.4 mm[ET]
   Dry Matter Productivity                  2.16 kg[DM]/m3[ET]            =   21.6 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.79 kg[grain yield]/m3[ET]   =    7.9 kg[yield]/ha per mm[ET]

 Transpiration during growing season       377.4 mm[EP]
   Dry Matter Productivity                  3.47 kg[DM]/m3[EP]            =   34.7 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.28 kg[grain yield]/m3[EP]   =   12.8 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                  87.3 kg[DM]/kg[N applied]
   Yield Productivity                       32.1 kg[yield]/kg[N applied]

 N uptake during growing season              162 kg[N uptake]/ha
   Dry Matter Productivity                  80.8 kg[DM]/kg[N uptake]
   Yield Productivity                       29.7 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     4819 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:38

*RUN  17        : SAMPLE 17                 MZCER048 EXPEFILE   17
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 17   : SAMPLE 17                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEAQ   2021
 SOIL           : IB00000008     TEXTURE : yLoam -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:245.9mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.210 0.333 0.426 0.123 0.333   1.00   1.19   5.85   0.00   0.00   2.55
  5- 15 0.222 0.346 0.432 0.124 0.346   0.85   1.20   5.93   0.00   0.00   2.16
 15- 30 0.226 0.350 0.433 0.124 0.350   0.70   1.22   5.88   0.00   0.00   1.58
 30- 45 0.239 0.364 0.441 0.125 0.364   0.50   1.28   5.99   0.00   0.00   1.01
 45- 60 0.239 0.364 0.441 0.125 0.364   0.50   1.28   5.99   0.00   0.00   1.01
 60- 80 0.240 0.363 0.440 0.123 0.363   0.38   1.34   6.14   0.00   0.00   0.59
 80-100 0.240 0.363 0.440 0.123 0.363   0.38   1.34   6.14   0.00   0.00   0.59
100-150 0.230 0.352 0.433 0.122 0.352   0.05   1.39   6.33   0.00   0.00   0.33
150-200 0.230 0.352 0.433 0.122 0.352   0.05   1.39   6.33   0.00   0.00   0.33

TOT-200  46.4  71.0  87.1  24.6  71.0  <--cm   -  kg/ha-->    0.0    0.0 186285
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    17     SAMPLE 17               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  2 MAY    6 Emergence       16   0.00   2.1    1  4.4  0.00  0.01  0.00  0.00     1
 26 MAY   30 End Juveni     275   0.48  10.6   10  3.6  0.00  0.00  0.00  0.00     2
 31 MAY   35 Floral Ini     517   0.79  12.3   19  3.6  0.00  0.00  0.00  0.00     3
 20 JUL   85 75% Silkin    4996   1.84  25.4   75  1.5  0.18  0.23  0.00  0.00     4
  5 AUG  101 Beg Gr Fil    7298   1.53  25.4   77  1.1  0.00  0.23  0.00  0.00     5
  3 OCT  160 End Gr Fil   11358   0.73  25.4   91  0.8  0.00  0.26  0.00  0.00     6
  7 OCT  164 Maturity     11358   0.73  25.4   91  0.8  0.00  0.47  0.00  0.00    10
  7 OCT  164 Harvest      11358   0.73  25.4   91  0.8  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                      85          -99
      Physiological maturity day (dap)                       164          -99
      Yield at harvest maturity (kg [dm]/ha)                4130          -99
      Number at maturity (no/m2)                            1167          -99
      Unit wt at maturity (g [dm]/unit)                   0.3540          -99
      Number at maturity (no/unit)                         291.7          -99
      Tops weight at maturity (kg [dm]/ha)                 11358          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    7281          -99
      Leaf area index, maximum                              2.19          -99
      Harvest index at maturity                            0.364          -99
      Grain N at maturity (kg/ha)                             57          -99
      Tops N at maturity (kg/ha)                              91          -99
      Stem N at maturity (kg/ha)                              35          -99
      Grain N at maturity (%)                                1.4          -99
      Tops weight at anthesis (kg [dm]/ha)                  4818          -99
      Tops N at anthesis (kg/ha)                              75          -99
      Leaf number per stem at maturity                     25.42          -99
      Emergence day (dap)                                      6          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    24  24.6  15.9  20.2  20.1  12.04  380.0  176.1   64.3  120.6    0    0    0    0    0   15  0.000  0.000  0.002  0.005  0.000  0.000
 End Juvenil-Floral Init    5  25.7  15.6  20.7  22.5  12.05  380.0    0.2   11.7   27.1    0    0    0    0    0    1  0.000  0.000  0.001  0.002  0.000  0.000
 Floral Init-End Lf Grow   50  25.1  15.1  20.1  19.1  12.05  380.0  203.0  142.9  214.7    0    0    0    0    0   23  0.130  0.180  0.090  0.224  0.000  0.000
 End Lf Grth-Beg Grn Fil   16  22.3  14.8  18.6  16.0  12.04  380.0  247.8   55.6   56.0    0    0    0    0    0   14  0.000  0.000  0.092  0.229  0.000  0.000
 Grain Filling Phase       59  25.2  14.8  20.0  21.7  12.02  380.0  186.5  250.7  293.8    0    0    0    0    0   42  0.000  0.000  0.102  0.254  0.000  0.000
   
 Planting to Harvest      164  24.9  15.1  20.0  20.2  12.03  380.0  818.4  547.3  769.9    0    0    0    0    0  100  0.040  0.055  0.078  0.195  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 164 days 

 Precipitation during growing season       818.4 mm[rain]
   Dry Matter Productivity                  1.39 kg[DM]/m3[rain]          =   13.9 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.50 kg[grain yield]/m3[rain] =    5.0 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  547.3 mm[ET]
   Dry Matter Productivity                  2.08 kg[DM]/m3[ET]            =   20.8 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.75 kg[grain yield]/m3[ET]   =    7.5 kg[yield]/ha per mm[ET]

 Transpiration during growing season       347.2 mm[EP]
   Dry Matter Productivity                  3.27 kg[DM]/m3[EP]            =   32.7 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.19 kg[grain yield]/m3[EP]   =   11.9 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                  75.7 kg[DM]/kg[N applied]
   Yield Productivity                       27.5 kg[yield]/kg[N applied]

 N uptake during growing season              143 kg[N uptake]/ha
   Dry Matter Productivity                  79.4 kg[DM]/kg[N uptake]
   Yield Productivity                       28.9 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     4130 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:38

*RUN  18        : SAMPLE 18                 MZCER048 EXPEFILE   18
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 18   : SAMPLE 18                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEAR   2021
 SOIL           : IB00000003     TEXTURE : ClayL -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:245.1mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.203 0.326 0.422 0.123 0.326   1.00   1.22   6.12   0.00   0.00   2.12
  5- 15 0.219 0.343 0.431 0.124 0.343   0.85   1.24   6.20   0.00   0.00   1.79
 15- 30 0.219 0.342 0.430 0.123 0.342   0.70   1.25   6.17   0.00   0.00   1.26
 30- 45 0.237 0.362 0.440 0.125 0.362   0.50   1.30   6.29   0.00   0.00   0.80
 45- 60 0.237 0.362 0.440 0.125 0.362   0.50   1.30   6.29   0.00   0.00   0.80
 60- 80 0.236 0.360 0.438 0.124 0.360   0.38   1.36   6.41   0.00   0.00   0.47
 80-100 0.236 0.360 0.438 0.124 0.360   0.38   1.36   6.41   0.00   0.00   0.47
100-150 0.224 0.345 0.430 0.121 0.345   0.05   1.41   6.59   0.00   0.00   0.27
150-200 0.224 0.345 0.430 0.121 0.345   0.05   1.41   6.59   0.00   0.00   0.27

TOT-200  45.4  69.9  86.6  24.5  69.9  <--cm   -  kg/ha-->    0.0    0.0 153591
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    18     SAMPLE 18               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  6 MAY   10 Emergence       16   0.00   1.8    1  4.4  0.00  0.00  0.00  0.00     1
 16 JUN   51 End Juveni     229   0.41  10.2    9  3.9  0.00  0.01  0.00  0.00     2
 21 JUN   56 Floral Ini     329   0.55  11.1   12  3.6  0.00  0.00  0.00  0.00     3
 23 SEP  150 75% Silkin    5975   2.51  23.2   88  1.5  0.00  0.07  0.00  0.00     4
 18 OCT  175 Beg Gr Fil   11637   1.59  23.2   91  0.8  0.03  0.36  0.00  0.00     5
 25 JAN  274 End Gr Fil   14172   0.05  23.2   80  0.6  0.66  0.56  0.00  0.00     6
 31 JAN  280 Maturity     14172   0.05  23.2   80  0.6  0.00  0.73  0.00  0.00    10
 31 JAN  280 Harvest      14172   0.05  23.2   80  0.6  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     150          -99
      Physiological maturity day (dap)                       280          -99
      Yield at harvest maturity (kg [dm]/ha)                4827          -99
      Number at maturity (no/m2)                            1252          -99
      Unit wt at maturity (g [dm]/unit)                   0.3854          -99
      Number at maturity (no/unit)                         313.1          -99
      Tops weight at maturity (kg [dm]/ha)                 14172          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    9400          -99
      Leaf area index, maximum                              2.81          -99
      Harvest index at maturity                            0.341          -99
      Grain N at maturity (kg/ha)                             50          -99
      Tops N at maturity (kg/ha)                              80          -99
      Stem N at maturity (kg/ha)                              30          -99
      Grain N at maturity (%)                                1.0          -99
      Tops weight at anthesis (kg [dm]/ha)                  5658          -99
      Tops N at anthesis (kg/ha)                              88          -99
      Leaf number per stem at maturity                     23.23          -99
      Emergence day (dap)                                     10          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    41  18.9  10.3  14.6  20.6  12.01  380.0  151.5   70.3  185.7    0    0    0    0    0   14  0.000  0.000  0.003  0.007  0.000  0.000
 End Juvenil-Floral Init    5  18.7  10.2  14.4  17.9  12.01  380.0    0.4    6.1   18.8    0    0    0    0    0    2  0.000  0.000  0.000  0.001  0.000  0.000
 Floral Init-End Lf Grow   94  18.0   9.8  13.9  18.4  12.01  380.0  519.3  303.9  339.2    0    0    0    0    0   75  0.000  0.000  0.026  0.065  0.000  0.000
 End Lf Grth-Beg Grn Fil   25  19.5  10.1  14.8  22.2  12.00  380.0   25.2  108.9  109.8    0    0    0    0    0   24  0.000  0.019  0.141  0.351  0.000  0.000
 Grain Filling Phase       99  20.3  10.2  15.2  22.5  11.99  380.0   47.3   78.2  482.0    0    0    0    0    0   45  0.545  0.664  0.274  0.554  0.000  0.000
   
 Planting to Harvest      280  19.2  10.1  14.7  20.8  12.00  380.0  779.9  592.8 1219.9    0    0    0    0    0  167  0.193  0.237  0.129  0.266  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 280 days 

 Precipitation during growing season       779.9 mm[rain]
   Dry Matter Productivity                  1.82 kg[DM]/m3[rain]          =   18.2 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.62 kg[grain yield]/m3[rain] =    6.2 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  592.8 mm[ET]
   Dry Matter Productivity                  2.39 kg[DM]/m3[ET]            =   23.9 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.81 kg[grain yield]/m3[ET]   =    8.1 kg[yield]/ha per mm[ET]

 Transpiration during growing season       374.7 mm[EP]
   Dry Matter Productivity                  3.78 kg[DM]/m3[EP]            =   37.8 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.29 kg[grain yield]/m3[EP]   =   12.9 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                  94.5 kg[DM]/kg[N applied]
   Yield Productivity                       32.2 kg[yield]/kg[N applied]

 N uptake during growing season              175 kg[N uptake]/ha
   Dry Matter Productivity                  81.0 kg[DM]/kg[N uptake]
   Yield Productivity                       27.6 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     4827 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:39

*RUN  19        : SAMPLE 19                 MZCER048 EXPEFILE   19
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 19   : SAMPLE 19                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEAS   2021
 SOIL           : IB00000012     TEXTURE : Loam  -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:252.0mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.215 0.341 0.430 0.126 0.341   1.00   1.13   5.53   0.00   0.00   3.44
  5- 15 0.233 0.359 0.440 0.126 0.359   0.85   1.15   5.60   0.00   0.00   2.91
 15- 30 0.237 0.365 0.443 0.128 0.365   0.70   1.17   5.57   0.00   0.00   2.20
 30- 45 0.259 0.386 0.456 0.127 0.386   0.50   1.22   5.67   0.00   0.00   1.41
 45- 60 0.259 0.386 0.456 0.127 0.386   0.50   1.22   5.67   0.00   0.00   1.41
 60- 80 0.258 0.385 0.455 0.127 0.385   0.38   1.29   5.79   0.00   0.00   0.81
 80-100 0.258 0.385 0.455 0.127 0.385   0.38   1.29   5.79   0.00   0.00   0.81
100-150 0.244 0.369 0.444 0.125 0.369   0.05   1.34   5.97   0.00   0.00   0.47
150-200 0.244 0.369 0.444 0.125 0.369   0.05   1.34   5.97   0.00   0.00   0.47

TOT-200  49.5  74.7  89.5  25.2  74.7  <--cm   -  kg/ha-->    0.0    0.0 247893
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    19     SAMPLE 19               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  2 MAY    6 Emergence       16   0.00   2.1    1  4.4  0.00  0.01  0.00  0.00     1
 26 MAY   30 End Juveni     274   0.47  10.6   10  3.6  0.00  0.00  0.00  0.00     2
 31 MAY   35 Floral Ini     516   0.79  12.3   19  3.6  0.00  0.00  0.00  0.00     3
 20 JUL   85 75% Silkin    5556   2.34  25.4   95  1.7  0.16  0.15  0.00  0.00     4
  5 AUG  101 Beg Gr Fil    8530   2.22  25.4   99  1.2  0.00  0.07  0.00  0.00     5
  3 OCT  160 End Gr Fil   13405   1.33  25.4  114  0.8  0.00  0.15  0.00  0.00     6
  7 OCT  164 Maturity     13405   1.33  25.4  114  0.8  0.00  0.45  0.00  0.00    10
  7 OCT  164 Harvest      13405   1.33  25.4  114  0.8  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                      85          -99
      Physiological maturity day (dap)                       164          -99
      Yield at harvest maturity (kg [dm]/ha)                4959          -99
      Number at maturity (no/m2)                            1401          -99
      Unit wt at maturity (g [dm]/unit)                   0.3540          -99
      Number at maturity (no/unit)                         350.2          -99
      Tops weight at maturity (kg [dm]/ha)                 13405          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    8501          -99
      Leaf area index, maximum                              2.36          -99
      Harvest index at maturity                            0.370          -99
      Grain N at maturity (kg/ha)                             75          -99
      Tops N at maturity (kg/ha)                             114          -99
      Stem N at maturity (kg/ha)                              38          -99
      Grain N at maturity (%)                                1.5          -99
      Tops weight at anthesis (kg [dm]/ha)                  5340          -99
      Tops N at anthesis (kg/ha)                              95          -99
      Leaf number per stem at maturity                     25.42          -99
      Emergence day (dap)                                      6          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    24  24.6  15.9  20.2  20.1  12.04  380.0  176.1   64.5  120.6    0    0    0    0    0   15  0.000  0.000  0.002  0.005  0.000  0.000
 End Juvenil-Floral Init    5  25.7  15.6  20.7  22.5  12.05  380.0    0.2   11.6   27.1    0    0    0    0    0    1  0.000  0.000  0.001  0.002  0.000  0.000
 Floral Init-End Lf Grow   50  25.1  15.1  20.1  19.1  12.05  380.0  203.0  148.0  214.0    0    0    0    0    0   23  0.104  0.161  0.060  0.151  0.000  0.000
 End Lf Grth-Beg Grn Fil   16  22.3  14.8  18.6  16.0  12.04  380.0  247.8   54.7   54.9    0    0    0    0    0   14  0.000  0.000  0.029  0.073  0.000  0.000
 Grain Filling Phase       59  25.2  14.8  20.0  21.7  12.02  380.0  186.5  276.9  284.2    0    0    0    0    0   42  0.000  0.000  0.056  0.140  0.000  0.000
   
 Planting to Harvest      164  24.9  15.1  20.0  20.2  12.03  380.0  818.4  579.8  757.8    0    0    0    0    0  100  0.032  0.049  0.046  0.115  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 164 days 

 Precipitation during growing season       818.4 mm[rain]
   Dry Matter Productivity                  1.64 kg[DM]/m3[rain]          =   16.4 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.61 kg[grain yield]/m3[rain] =    6.1 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  579.8 mm[ET]
   Dry Matter Productivity                  2.31 kg[DM]/m3[ET]            =   23.1 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.86 kg[grain yield]/m3[ET]   =    8.6 kg[yield]/ha per mm[ET]

 Transpiration during growing season       417.5 mm[EP]
   Dry Matter Productivity                  3.21 kg[DM]/m3[EP]            =   32.1 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.19 kg[grain yield]/m3[EP]   =   11.9 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                  89.4 kg[DM]/kg[N applied]
   Yield Productivity                       33.1 kg[yield]/kg[N applied]

 N uptake during growing season              181 kg[N uptake]/ha
   Dry Matter Productivity                  74.1 kg[DM]/kg[N uptake]
   Yield Productivity                       27.4 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     4959 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:39

*RUN  20        : SAMPLE 20                 MZCER048 EXPEFILE   20
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 20   : SAMPLE 20                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEAT   2021
 SOIL           : IB00000013     TEXTURE : yLoam -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:248.6mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.220 0.346 0.433 0.126 0.346   1.00   1.21   6.15   0.00   0.00   1.90
  5- 15 0.232 0.359 0.439 0.127 0.359   0.85   1.23   6.21   0.00   0.00   1.61
 15- 30 0.232 0.357 0.438 0.125 0.357   0.70   1.24   6.20   0.00   0.00   1.15
 30- 45 0.245 0.371 0.445 0.126 0.371   0.50   1.29   6.33   0.00   0.00   0.73
 45- 60 0.245 0.371 0.445 0.126 0.371   0.50   1.29   6.33   0.00   0.00   0.73
 60- 80 0.245 0.370 0.444 0.125 0.370   0.38   1.36   6.48   0.00   0.00   0.43
 80-100 0.245 0.370 0.444 0.125 0.370   0.38   1.36   6.48   0.00   0.00   0.43
100-150 0.236 0.359 0.437 0.123 0.359   0.05   1.41   6.67   0.00   0.00   0.25
150-200 0.236 0.359 0.437 0.123 0.359   0.05   1.41   6.67   0.00   0.00   0.25

TOT-200  47.7  72.5  87.9  24.9  72.5  <--cm   -  kg/ha-->    0.0    0.0 139581
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    20     SAMPLE 20               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  2 MAY    6 Emergence       16   0.00   2.2    1  4.4  0.00  0.01  0.00  0.00     1
 21 MAY   25 End Juveni     243   0.43  10.3    9  3.5  0.00  0.00  0.00  0.00     2
 26 MAY   30 Floral Ini     510   0.78  12.2   18  3.6  0.00  0.00  0.00  0.00     3
  5 JUL   70 75% Silkin    4317   1.62  25.4   52  1.2  0.29  0.30  0.00  0.00     4
 18 JUL   83 Beg Gr Fil    5873   1.52  25.4   92  1.6  0.12  0.09  0.00  0.00     5
  7 SEP  134 End Gr Fil    8878   1.20  25.4   93  1.0  0.00  0.00  0.00  0.00     6
 10 SEP  137 Maturity      8878   1.20  25.4   93  1.0  0.00  0.00  0.00  0.00    10
 10 SEP  137 Harvest       8878   1.20  25.4   93  1.0  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                      70          -99
      Physiological maturity day (dap)                       137          -99
      Yield at harvest maturity (kg [dm]/ha)                3065          -99
      Number at maturity (no/m2)                            1002          -99
      Unit wt at maturity (g [dm]/unit)                   0.3060          -99
      Number at maturity (no/unit)                         250.4          -99
      Tops weight at maturity (kg [dm]/ha)                  8878          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    5864          -99
      Leaf area index, maximum                              2.29          -99
      Harvest index at maturity                            0.345          -99
      Grain N at maturity (kg/ha)                             52          -99
      Tops N at maturity (kg/ha)                              93          -99
      Stem N at maturity (kg/ha)                              41          -99
      Grain N at maturity (%)                                1.7          -99
      Tops weight at anthesis (kg [dm]/ha)                  4251          -99
      Tops N at anthesis (kg/ha)                              52          -99
      Leaf number per stem at maturity                     25.39          -99
      Emergence day (dap)                                      6          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    19  26.9  18.6  22.8  19.5  12.03  380.0  152.1   56.6   97.7    0    0    1    0    0   12  0.000  0.000  0.002  0.004  0.000  0.000
 End Juvenil-Floral Init    5  27.6  18.4  23.0  21.6  12.04  380.0   12.0   11.9   27.3    0    0    0    0    0    3  0.000  0.000  0.000  0.001  0.000  0.000
 Floral Init-End Lf Grow   40  28.3  17.8  23.1  21.2  12.04  380.0    3.6  119.2  200.7    0    0    0    0    0    9  0.208  0.274  0.127  0.291  0.000  0.000
 End Lf Grth-Beg Grn Fil   13  25.2  17.7  21.4  15.3  12.04  380.0  122.9   38.3   46.1    0    0    0    0    0   13  0.133  0.175  0.056  0.130  0.000  0.000
 Grain Filling Phase       51  26.5  17.5  22.0  19.4  12.02  380.0  454.5  219.0  232.7    0    0    0    0    0   39  0.000  0.000  0.000  0.000  0.000  0.000
   
 Planting to Harvest      137  27.2  17.8  22.5  19.8  12.03  380.0  752.9  466.4  657.4    0    0    4    0    0   79  0.073  0.097  0.043  0.098  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 137 days 

 Precipitation during growing season       752.9 mm[rain]
   Dry Matter Productivity                  1.18 kg[DM]/m3[rain]          =   11.8 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.41 kg[grain yield]/m3[rain] =    4.1 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  466.4 mm[ET]
   Dry Matter Productivity                  1.90 kg[DM]/m3[ET]            =   19.0 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.66 kg[grain yield]/m3[ET]   =    6.6 kg[yield]/ha per mm[ET]

 Transpiration during growing season       306.4 mm[EP]
   Dry Matter Productivity                  2.90 kg[DM]/m3[EP]            =   29.0 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.00 kg[grain yield]/m3[EP]   =   10.0 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                  59.2 kg[DM]/kg[N applied]
   Yield Productivity                       20.4 kg[yield]/kg[N applied]

 N uptake during growing season              145 kg[N uptake]/ha
   Dry Matter Productivity                  61.2 kg[DM]/kg[N uptake]
   Yield Productivity                       21.1 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     3065 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:39

*RUN  21        : SAMPLE 21                 MZCER048 EXPEFILE   21
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 21   : SAMPLE 21                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEAU   2021
 SOIL           : IB00000014     TEXTURE : Loam  -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:247.6mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.203 0.326 0.423 0.123 0.326   1.00   1.17   5.74   0.00   0.00   2.89
  5- 15 0.222 0.347 0.433 0.125 0.347   0.85   1.19   5.81   0.00   0.00   2.45
 15- 30 0.223 0.348 0.433 0.125 0.348   0.70   1.20   5.78   0.00   0.00   1.82
 30- 45 0.243 0.369 0.444 0.126 0.369   0.50   1.26   5.88   0.00   0.00   1.17
 45- 60 0.243 0.369 0.444 0.126 0.369   0.50   1.26   5.88   0.00   0.00   1.17
 60- 80 0.243 0.369 0.444 0.126 0.369   0.38   1.33   6.02   0.00   0.00   0.68
 80-100 0.243 0.369 0.444 0.126 0.369   0.38   1.33   6.02   0.00   0.00   0.68
100-150 0.229 0.351 0.433 0.122 0.351   0.05   1.39   6.19   0.00   0.00   0.38
150-200 0.229 0.351 0.433 0.122 0.351   0.05   1.39   6.19   0.00   0.00   0.38

TOT-200  46.5  71.2  87.3  24.8  71.2  <--cm   -  kg/ha-->    0.0    0.0 212044
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    21     SAMPLE 21               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  6 MAY   10 Emergence       16   0.00   1.8    1  4.4  0.00  0.00  0.00  0.00     1
 16 JUN   51 End Juveni     229   0.41  10.2    9  3.9  0.00  0.01  0.00  0.00     2
 21 JUN   56 Floral Ini     329   0.55  11.1   12  3.6  0.00  0.00  0.00  0.00     3
 23 SEP  150 75% Silkin    6166   2.82  23.2   94  1.5  0.00  0.04  0.00  0.00     4
 18 OCT  175 Beg Gr Fil   12470   1.93  23.2   99  0.8  0.03  0.30  0.00  0.00     5
 25 JAN  274 End Gr Fil   15142   0.06  23.2   86  0.6  0.71  0.53  0.00  0.00     6
 31 JAN  280 Maturity     15142   0.06  23.2   86  0.6  0.00  0.74  0.00  0.00    10
 31 JAN  280 Harvest      15142   0.06  23.2   86  0.6  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     150          -99
      Physiological maturity day (dap)                       280          -99
      Yield at harvest maturity (kg [dm]/ha)                5332          -99
      Number at maturity (no/m2)                            1470          -99
      Unit wt at maturity (g [dm]/unit)                   0.3627          -99
      Number at maturity (no/unit)                         367.6          -99
      Tops weight at maturity (kg [dm]/ha)                 15142          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    9865          -99
      Leaf area index, maximum                              2.94          -99
      Harvest index at maturity                            0.352          -99
      Grain N at maturity (kg/ha)                             58          -99
      Tops N at maturity (kg/ha)                              86          -99
      Stem N at maturity (kg/ha)                              28          -99
      Grain N at maturity (%)                                1.1          -99
      Tops weight at anthesis (kg [dm]/ha)                  5825          -99
      Tops N at anthesis (kg/ha)                              94          -99
      Leaf number per stem at maturity                     23.23          -99
      Emergence day (dap)                                     10          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    41  18.9  10.3  14.6  20.6  12.01  380.0  151.5   70.5  185.7    0    0    0    0    0   14  0.000  0.000  0.003  0.007  0.000  0.000
 End Juvenil-Floral Init    5  18.7  10.2  14.4  17.9  12.01  380.0    0.4    6.1   18.8    0    0    0    0    0    2  0.000  0.000  0.000  0.001  0.000  0.000
 Floral Init-End Lf Grow   94  18.0   9.8  13.9  18.4  12.01  380.0  519.3  303.5  338.9    0    0    0    0    0   75  0.000  0.000  0.016  0.041  0.000  0.000
 End Lf Grth-Beg Grn Fil   25  19.5  10.1  14.8  22.2  12.00  380.0   25.2  108.3  108.9    0    0    0    0    0   24  0.000  0.014  0.117  0.293  0.000  0.000
 Grain Filling Phase       99  20.3  10.2  15.2  22.5  11.99  380.0   47.3   83.1  479.3    0    0    0    0    0   45  0.618  0.715  0.258  0.527  0.000  0.000
   
 Planting to Harvest      280  19.2  10.1  14.7  20.8  12.00  380.0  779.9  597.1 1216.1    0    0    0    0    0  167  0.218  0.254  0.118  0.243  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 280 days 

 Precipitation during growing season       779.9 mm[rain]
   Dry Matter Productivity                  1.94 kg[DM]/m3[rain]          =   19.4 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.68 kg[grain yield]/m3[rain] =    6.8 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  597.1 mm[ET]
   Dry Matter Productivity                  2.54 kg[DM]/m3[ET]            =   25.4 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.89 kg[grain yield]/m3[ET]   =    8.9 kg[yield]/ha per mm[ET]

 Transpiration during growing season       385.0 mm[EP]
   Dry Matter Productivity                  3.93 kg[DM]/m3[EP]            =   39.3 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.39 kg[grain yield]/m3[EP]   =   13.9 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                 100.9 kg[DM]/kg[N applied]
   Yield Productivity                       35.5 kg[yield]/kg[N applied]

 N uptake during growing season              190 kg[N uptake]/ha
   Dry Matter Productivity                  79.7 kg[DM]/kg[N uptake]
   Yield Productivity                       28.1 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     5332 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:39

*RUN  22        : SAMPLE 22                 MZCER048 EXPEFILE   22
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 22   : SAMPLE 22                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEAV   2021
 SOIL           : IB00000013     TEXTURE : yLoam -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:248.6mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.220 0.346 0.433 0.126 0.346   1.00   1.21   6.15   0.00   0.00   1.90
  5- 15 0.232 0.359 0.439 0.127 0.359   0.85   1.23   6.21   0.00   0.00   1.61
 15- 30 0.232 0.357 0.438 0.125 0.357   0.70   1.24   6.20   0.00   0.00   1.15
 30- 45 0.245 0.371 0.445 0.126 0.371   0.50   1.29   6.33   0.00   0.00   0.73
 45- 60 0.245 0.371 0.445 0.126 0.371   0.50   1.29   6.33   0.00   0.00   0.73
 60- 80 0.245 0.370 0.444 0.125 0.370   0.38   1.36   6.48   0.00   0.00   0.43
 80-100 0.245 0.370 0.444 0.125 0.370   0.38   1.36   6.48   0.00   0.00   0.43
100-150 0.236 0.359 0.437 0.123 0.359   0.05   1.41   6.67   0.00   0.00   0.25
150-200 0.236 0.359 0.437 0.123 0.359   0.05   1.41   6.67   0.00   0.00   0.25

TOT-200  47.7  72.5  87.9  24.9  72.5  <--cm   -  kg/ha-->    0.0    0.0 139581
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    22     SAMPLE 22               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  4 MAY    8 Emergence       16   0.00   1.9    1  4.4  0.00  0.00  0.00  0.00     1
  4 JUN   39 End Juveni     243   0.43  10.3    9  3.8  0.00  0.00  0.00  0.00     2
  9 JUN   44 Floral Ini     388   0.63  11.5   14  3.6  0.00  0.00  0.00  0.00     3
 18 AUG  114 75% Silkin    6422   3.20  24.1  107  1.7  0.01  0.05  0.00  0.00     4
  7 SEP  134 Beg Gr Fil   12088   2.73  24.1  110  0.9  0.00  0.16  0.00  0.00     5
 20 NOV  208 End Gr Fil   17607   0.33  24.1  108  0.6  0.49  0.51  0.00  0.00     6
 25 NOV  213 Maturity     17607   0.33  24.1  108  0.6  0.81  0.80  0.00  0.00    10
 25 NOV  213 Harvest      17607   0.33  24.1  108  0.6  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     114          -99
      Physiological maturity day (dap)                       213          -99
      Yield at harvest maturity (kg [dm]/ha)                6614          -99
      Number at maturity (no/m2)                            1927          -99
      Unit wt at maturity (g [dm]/unit)                   0.3432          -99
      Number at maturity (no/unit)                         481.7          -99
      Tops weight at maturity (kg [dm]/ha)                 17607          -99
      By-product produced (stalk) at maturity (kg[dm]/ha   11050          -99
      Leaf area index, maximum                              3.22          -99
      Harvest index at maturity                            0.376          -99
      Grain N at maturity (kg/ha)                             76          -99
      Tops N at maturity (kg/ha)                             108          -99
      Stem N at maturity (kg/ha)                              32          -99
      Grain N at maturity (%)                                1.1          -99
      Tops weight at anthesis (kg [dm]/ha)                  6073          -99
      Tops N at anthesis (kg/ha)                             107          -99
      Leaf number per stem at maturity                     24.07          -99
      Emergence day (dap)                                      8          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    31  21.4  12.6  17.0  20.4  12.01  380.0  166.7   76.0  147.5    0    0    0    0    0   15  0.000  0.000  0.002  0.005  0.000  0.000
 End Juvenil-Floral Init    5  22.2  11.8  17.0  22.2  12.01  380.0    0.0    7.6   24.9    0    0    0    0    0    0  0.000  0.000  0.001  0.001  0.000  0.000
 Floral Init-End Lf Grow   70  20.5  11.8  16.1  18.1  12.01  380.0  442.2  211.7  260.1    0    0    0    0    0   50  0.001  0.009  0.018  0.044  0.000  0.000
 End Lf Grth-Beg Grn Fil   20  21.5  11.6  16.5  21.4  12.00  380.0   49.0   86.3   86.8    0    0    0    0    0   14  0.000  0.000  0.062  0.155  0.000  0.000
 Grain Filling Phase       74  23.0  12.3  17.6  22.5  12.00  380.0   59.0  182.5  361.5    0    0    0    0    0   56  0.412  0.481  0.265  0.500  0.000  0.000
   
 Planting to Harvest      213  21.8  12.1  17.0  20.6  12.00  380.0  738.2  582.0  951.2    0    0    0    0    0  141  0.160  0.189  0.117  0.222  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 213 days 

 Precipitation during growing season       738.2 mm[rain]
   Dry Matter Productivity                  2.39 kg[DM]/m3[rain]          =   23.9 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.90 kg[grain yield]/m3[rain] =    9.0 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  582.0 mm[ET]
   Dry Matter Productivity                  3.03 kg[DM]/m3[ET]            =   30.3 kg[DM]/ha per mm[ET]
   Yield Productivity                       1.14 kg[grain yield]/m3[ET]   =   11.4 kg[yield]/ha per mm[ET]

 Transpiration during growing season       411.6 mm[EP]
   Dry Matter Productivity                  4.28 kg[DM]/m3[EP]            =   42.8 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.61 kg[grain yield]/m3[EP]   =   16.1 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                 117.4 kg[DM]/kg[N applied]
   Yield Productivity                       44.1 kg[yield]/kg[N applied]

 N uptake during growing season              187 kg[N uptake]/ha
   Dry Matter Productivity                  94.2 kg[DM]/kg[N uptake]
   Yield Productivity                       35.4 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     6614 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:39

*RUN  23        : SAMPLE 23                 MZCER048 EXPEFILE   23
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 23   : SAMPLE 23                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEAW   2021
 SOIL           : IB00000006     TEXTURE : ClayL -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:243.3mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.203 0.325 0.422 0.122 0.325   1.00   1.23   6.19   0.00   0.00   1.96
  5- 15 0.219 0.343 0.430 0.124 0.343   0.85   1.25   6.25   0.00   0.00   1.64
 15- 30 0.219 0.342 0.429 0.123 0.342   0.70   1.26   6.21   0.00   0.00   1.15
 30- 45 0.236 0.360 0.438 0.124 0.360   0.50   1.31   6.32   0.00   0.00   0.74
 45- 60 0.236 0.360 0.438 0.124 0.360   0.50   1.31   6.32   0.00   0.00   0.74
 60- 80 0.236 0.359 0.437 0.123 0.359   0.38   1.37   6.45   0.00   0.00   0.43
 80-100 0.236 0.359 0.437 0.123 0.359   0.38   1.37   6.45   0.00   0.00   0.43
100-150 0.223 0.343 0.429 0.120 0.343   0.05   1.42   6.63   0.00   0.00   0.24
150-200 0.223 0.343 0.429 0.120 0.343   0.05   1.42   6.63   0.00   0.00   0.24

TOT-200  45.3  69.6  86.4  24.3  69.6  <--cm   -  kg/ha-->    0.0    0.0 141015
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    23     SAMPLE 23               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  2 MAY    6 Emergence       16   0.00   2.1    1  4.4  0.00  0.01  0.00  0.00     1
 26 MAY   30 End Juveni     275   0.48  10.6   10  3.6  0.00  0.00  0.00  0.00     2
 31 MAY   35 Floral Ini     517   0.79  12.3   19  3.6  0.00  0.00  0.00  0.00     3
 20 JUL   85 75% Silkin    5066   1.91  25.4   85  1.7  0.18  0.22  0.00  0.00     4
  5 AUG  101 Beg Gr Fil    7772   1.83  25.4   87  1.1  0.00  0.05  0.00  0.00     5
  3 OCT  160 End Gr Fil   12285   1.01  25.4   96  0.8  0.00  0.21  0.00  0.00     6
  7 OCT  164 Maturity     12285   1.01  25.4   96  0.8  0.00  0.59  0.00  0.00    10
  7 OCT  164 Harvest      12285   1.01  25.4   96  0.8  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                      85          -99
      Physiological maturity day (dap)                       164          -99
      Yield at harvest maturity (kg [dm]/ha)                4591          -99
      Number at maturity (no/m2)                            1297          -99
      Unit wt at maturity (g [dm]/unit)                   0.3540          -99
      Number at maturity (no/unit)                         324.2          -99
      Tops weight at maturity (kg [dm]/ha)                 12285          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    7746          -99
      Leaf area index, maximum                              1.95          -99
      Harvest index at maturity                            0.374          -99
      Grain N at maturity (kg/ha)                             66          -99
      Tops N at maturity (kg/ha)                              96          -99
      Stem N at maturity (kg/ha)                              30          -99
      Grain N at maturity (%)                                1.4          -99
      Tops weight at anthesis (kg [dm]/ha)                  4868          -99
      Tops N at anthesis (kg/ha)                              85          -99
      Leaf number per stem at maturity                     25.42          -99
      Emergence day (dap)                                      6          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    24  24.6  15.9  20.2  20.1  12.04  380.0  176.1   64.2  120.6    0    0    0    0    0   15  0.000  0.000  0.002  0.005  0.000  0.000
 End Juvenil-Floral Init    5  25.7  15.6  20.7  22.5  12.05  380.0    0.2   11.7   27.1    0    0    0    0    0    1  0.000  0.000  0.001  0.002  0.000  0.000
 Floral Init-End Lf Grow   50  25.1  15.1  20.1  19.1  12.05  380.0  203.0  140.9  215.4    0    0    0    0    0   23  0.133  0.181  0.091  0.216  0.000  0.000
 End Lf Grth-Beg Grn Fil   16  22.3  14.8  18.6  16.0  12.04  380.0  247.8   55.4   55.6    0    0    0    0    0   14  0.000  0.000  0.023  0.057  0.000  0.000
 Grain Filling Phase       59  25.2  14.8  20.0  21.7  12.02  380.0  186.5  267.5  288.1    0    0    0    0    0   42  0.000  0.000  0.083  0.199  0.000  0.000
   
 Planting to Harvest      164  24.9  15.1  20.0  20.2  12.03  380.0  818.4  563.6  764.2    0    0    0    0    0  100  0.041  0.055  0.068  0.158  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 164 days 

 Precipitation during growing season       818.4 mm[rain]
   Dry Matter Productivity                  1.50 kg[DM]/m3[rain]          =   15.0 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.56 kg[grain yield]/m3[rain] =    5.6 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  563.6 mm[ET]
   Dry Matter Productivity                  2.18 kg[DM]/m3[ET]            =   21.8 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.81 kg[grain yield]/m3[ET]   =    8.1 kg[yield]/ha per mm[ET]

 Transpiration during growing season       379.5 mm[EP]
   Dry Matter Productivity                  3.24 kg[DM]/m3[EP]            =   32.4 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.21 kg[grain yield]/m3[EP]   =   12.1 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                  81.9 kg[DM]/kg[N applied]
   Yield Productivity                       30.6 kg[yield]/kg[N applied]

 N uptake during growing season              152 kg[N uptake]/ha
   Dry Matter Productivity                  80.8 kg[DM]/kg[N uptake]
   Yield Productivity                       30.2 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     4591 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:39

*RUN  24        : SAMPLE 24                 MZCER048 EXPEFILE   24
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 24   : SAMPLE 24                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEAX   2021
 SOIL           : IB00000007     TEXTURE : Loam  -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:248.6mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.204 0.327 0.423 0.123 0.327   1.00   1.17   5.74   0.00   0.00   2.89
  5- 15 0.221 0.346 0.432 0.125 0.346   0.85   1.19   5.81   0.00   0.00   2.45
 15- 30 0.223 0.348 0.433 0.125 0.348   0.70   1.20   5.78   0.00   0.00   1.82
 30- 45 0.242 0.368 0.444 0.126 0.368   0.50   1.26   5.89   0.00   0.00   1.17
 45- 60 0.242 0.368 0.444 0.126 0.368   0.50   1.26   5.89   0.00   0.00   1.17
 60- 80 0.241 0.367 0.443 0.126 0.367   0.38   1.33   6.03   0.00   0.00   0.68
 80-100 0.241 0.367 0.443 0.126 0.367   0.38   1.33   6.03   0.00   0.00   0.68
100-150 0.228 0.351 0.433 0.123 0.351   0.05   1.38   6.21   0.00   0.00   0.39
150-200 0.228 0.351 0.433 0.123 0.351   0.05   1.38   6.21   0.00   0.00   0.39

TOT-200  46.3  71.1  87.3  24.9  71.1  <--cm   -  kg/ha-->    0.0    0.0 213044
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    24     SAMPLE 24               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  3 MAY    7 Emergence       16   0.00   2.0    1  4.4  0.00  0.01  0.00  0.00     1
 28 MAY   32 End Juveni     244   0.43  10.3    9  3.7  0.00  0.00  0.00  0.00     2
  2 JUN   37 Floral Ini     448   0.70  11.9   16  3.6  0.00  0.00  0.00  0.00     3
 26 JUL   91 75% Silkin    5164   1.99  24.9   76  1.5  0.10  0.19  0.00  0.00     4
 12 AUG  108 Beg Gr Fil    8145   1.54  24.9   80  1.0  0.00  0.30  0.00  0.00     5
 14 OCT  171 End Gr Fil   12595   0.56  24.9   98  0.8  0.00  0.33  0.00  0.00     6
 18 OCT  175 Maturity     12595   0.56  24.9   98  0.8  0.00  0.46  0.00  0.00    10
 18 OCT  175 Harvest      12595   0.56  24.9   98  0.8  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                      91          -99
      Physiological maturity day (dap)                       175          -99
      Yield at harvest maturity (kg [dm]/ha)                4527          -99
      Number at maturity (no/m2)                            1196          -99
      Unit wt at maturity (g [dm]/unit)                   0.3785          -99
      Number at maturity (no/unit)                         299.0          -99
      Tops weight at maturity (kg [dm]/ha)                 12595          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    8120          -99
      Leaf area index, maximum                              2.12          -99
      Harvest index at maturity                            0.359          -99
      Grain N at maturity (kg/ha)                             58          -99
      Tops N at maturity (kg/ha)                              98          -99
      Stem N at maturity (kg/ha)                              40          -99
      Grain N at maturity (%)                                1.3          -99
      Tops weight at anthesis (kg [dm]/ha)                  5091          -99
      Tops N at anthesis (kg/ha)                              76          -99
      Leaf number per stem at maturity                     24.89          -99
      Emergence day (dap)                                      7          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    25  23.4  15.1  19.2  20.5  12.05  380.0  216.9   63.2  125.2    0    0    0    0    0   15  0.000  0.000  0.002  0.005  0.000  0.000
 End Juvenil-Floral Init    5  25.3  14.6  20.0  23.0  12.06  380.0    1.4   11.3   27.4    0    0    0    0    0    2  0.000  0.000  0.001  0.001  0.000  0.000
 Floral Init-End Lf Grow   54  23.5  14.4  18.9  18.7  12.06  380.0  470.4  164.5  222.6    0    0    0    0    0   31  0.056  0.097  0.075  0.186  0.000  0.000
 End Lf Grth-Beg Grn Fil   17  21.9  14.3  18.1  18.3  12.04  380.0  144.0   66.2   66.7    0    0    0    0    0   13  0.000  0.000  0.119  0.298  0.000  0.000
 Grain Filling Phase       63  24.0  14.4  19.2  22.0  12.01  380.0  350.0  279.8  315.9    0    0    0    0    0   53  0.000  0.000  0.130  0.324  0.000  0.000
   
 Planting to Harvest      175  23.7  14.5  19.1  20.5  12.03  380.0   1184    604    822    0    0    0    0    0  117  0.017  0.030  0.086  0.214  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 175 days 

 Precipitation during growing season      1183.7 mm[rain]
   Dry Matter Productivity                  1.06 kg[DM]/m3[rain]          =   10.6 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.38 kg[grain yield]/m3[rain] =    3.8 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  603.7 mm[ET]
   Dry Matter Productivity                  2.09 kg[DM]/m3[ET]            =   20.9 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.75 kg[grain yield]/m3[ET]   =    7.5 kg[yield]/ha per mm[ET]

 Transpiration during growing season       365.9 mm[EP]
   Dry Matter Productivity                  3.44 kg[DM]/m3[EP]            =   34.4 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.24 kg[grain yield]/m3[EP]   =   12.4 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                  84.0 kg[DM]/kg[N applied]
   Yield Productivity                       30.2 kg[yield]/kg[N applied]

 N uptake during growing season              155 kg[N uptake]/ha
   Dry Matter Productivity                  81.3 kg[DM]/kg[N uptake]
   Yield Productivity                       29.2 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     4527 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:39

*RUN  25        : SAMPLE 25                 MZCER048 EXPEFILE   25
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 25   : SAMPLE 25                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEAY   2021
 SOIL           : IB00000010     TEXTURE : ClayL -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:254.5mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.231 0.358 0.439 0.127 0.358   1.00   1.15   5.60   0.00   0.00   3.03
  5- 15 0.248 0.375 0.449 0.127 0.375   0.85   1.16   5.66   0.00   0.00   2.57
 15- 30 0.252 0.380 0.452 0.128 0.380   0.70   1.18   5.63   0.00   0.00   1.95
 30- 45 0.272 0.400 0.465 0.128 0.400   0.50   1.24   5.75   0.00   0.00   1.25
 45- 60 0.272 0.400 0.465 0.128 0.400   0.50   1.24   5.75   0.00   0.00   1.25
 60- 80 0.271 0.398 0.463 0.127 0.398   0.38   1.31   5.88   0.00   0.00   0.73
 80-100 0.271 0.398 0.463 0.127 0.398   0.38   1.31   5.88   0.00   0.00   0.73
100-150 0.257 0.384 0.453 0.127 0.384   0.05   1.36   6.06   0.00   0.00   0.42
150-200 0.257 0.384 0.453 0.127 0.384   0.05   1.36   6.06   0.00   0.00   0.42

TOT-200  52.1  77.6  91.2  25.4  77.6  <--cm   -  kg/ha-->    0.0    0.0 223622
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    25     SAMPLE 25               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  5 MAY    9 Emergence       16   0.00   1.8    1  4.4  0.00  0.00  0.00  0.00     1
 10 JUN   45 End Juveni     226   0.40  10.1    9  3.8  0.00  0.01  0.00  0.00     2
 15 JUN   50 Floral Ini     346   0.57  11.2   12  3.6  0.00  0.00  0.00  0.00     3
  7 SEP  134 75% Silkin    5009   2.16  23.5   69  1.4  0.00  0.08  0.00  0.00     4
  1 OCT  158 Beg Gr Fil    9642   1.30  23.5   71  0.7  0.00  0.42  0.00  0.00     5
 28 DEC  246 End Gr Fil   13930   0.10  23.5   85  0.6  0.00  0.57  0.00  0.00     6
  3 JAN  252 Maturity     13930   0.10  23.5   85  0.6  0.00  0.60  0.00  0.00    10
  3 JAN  252 Harvest      13930   0.10  23.5   85  0.6  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     134          -99
      Physiological maturity day (dap)                       252          -99
      Yield at harvest maturity (kg [dm]/ha)                5155          -99
      Number at maturity (no/m2)                             987          -99
      Unit wt at maturity (g [dm]/unit)                   0.5221          -99
      Number at maturity (no/unit)                         246.8          -99
      Tops weight at maturity (kg [dm]/ha)                 13930          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    8825          -99
      Leaf area index, maximum                              2.49          -99
      Harvest index at maturity                            0.370          -99
      Grain N at maturity (kg/ha)                             49          -99
      Tops N at maturity (kg/ha)                              85          -99
      Stem N at maturity (kg/ha)                              36          -99
      Grain N at maturity (%)                                1.0          -99
      Tops weight at anthesis (kg [dm]/ha)                  4751          -99
      Tops N at anthesis (kg/ha)                              69          -99
      Leaf number per stem at maturity                     23.45          -99
      Emergence day (dap)                                      9          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    36  19.7  11.4  15.5  20.8  12.05  380.0  367.2  100.4  169.7    0    0    0    0    0   27  0.000  0.000  0.003  0.006  0.000  0.000
 End Juvenil-Floral Init    5  20.5  11.1  15.8  22.1  12.05  380.0    3.0    9.3   24.2    0    0    0    0    0    4  0.000  0.000  0.000  0.000  0.000  0.000
 Floral Init-End Lf Grow   84  18.7  10.7  14.7  19.0  12.04  380.0   1654    308    321    0    0    0    0    0   81  0.000  0.000  0.032  0.080  0.000  0.000
 End Lf Grth-Beg Grn Fil   24  19.5  10.8  15.2  21.3  12.00  380.0  469.0  103.1  103.5    0    0    0    0    0   24  0.000  0.000  0.162  0.406  0.000  0.000
 Grain Filling Phase       88  20.7  11.3  16.0  22.8  11.96  380.0  691.4  248.8  441.4    0    0    0    0    0   70  0.000  0.000  0.286  0.572  0.000  0.000
   
 Planting to Harvest      252  19.8  11.1  15.5  21.1  12.01  380.0   3286    798   1138    0    0    0    0    0  216  0.000  0.000  0.134  0.280  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 252 days 

 Precipitation during growing season      3285.8 mm[rain]
   Dry Matter Productivity                  0.42 kg[DM]/m3[rain]          =    4.2 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.16 kg[grain yield]/m3[rain] =    1.6 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  797.8 mm[ET]
   Dry Matter Productivity                  1.75 kg[DM]/m3[ET]            =   17.5 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.65 kg[grain yield]/m3[ET]   =    6.5 kg[yield]/ha per mm[ET]

 Transpiration during growing season       425.8 mm[EP]
   Dry Matter Productivity                  3.27 kg[DM]/m3[EP]            =   32.7 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.21 kg[grain yield]/m3[EP]   =   12.1 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                  92.9 kg[DM]/kg[N applied]
   Yield Productivity                       34.4 kg[yield]/kg[N applied]

 N uptake during growing season              179 kg[N uptake]/ha
   Dry Matter Productivity                  77.8 kg[DM]/kg[N uptake]
   Yield Productivity                       28.8 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     5155 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:39

*RUN  26        : SAMPLE 26                 MZCER048 EXPEFILE   26
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 26   : SAMPLE 26                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEAZ   2021
 SOIL           : IB00000015     TEXTURE : Loam  -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:248.0mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.208 0.331 0.424 0.123 0.331   1.00   1.18   5.85   0.00   0.00   2.55
  5- 15 0.224 0.349 0.433 0.125 0.349   0.85   1.21   5.93   0.00   0.00   2.16
 15- 30 0.226 0.350 0.433 0.124 0.350   0.70   1.22   5.88   0.00   0.00   1.58
 30- 45 0.246 0.372 0.445 0.126 0.372   0.50   1.28   5.99   0.00   0.00   1.01
 45- 60 0.246 0.372 0.445 0.126 0.372   0.50   1.28   5.99   0.00   0.00   1.01
 60- 80 0.245 0.370 0.444 0.125 0.370   0.38   1.35   6.13   0.00   0.00   0.59
 80-100 0.245 0.370 0.444 0.125 0.370   0.38   1.35   6.13   0.00   0.00   0.59
100-150 0.232 0.355 0.435 0.123 0.355   0.05   1.42   6.29   0.00   0.00   0.34
150-200 0.232 0.355 0.435 0.123 0.355   0.05   1.42   6.29   0.00   0.00   0.34

TOT-200  47.0  71.9  87.6  24.8  71.9  <--cm   -  kg/ha-->    0.0    0.0 189019
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    26     SAMPLE 26               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  4 MAY    8 Emergence       16   0.00   1.9    1  4.4  0.00  0.00  0.00  0.00     1
  1 JUN   36 End Juveni     223   0.40  10.0    8  3.7  0.00  0.00  0.00  0.00     2
  6 JUN   41 Floral Ini     387   0.62  11.5   14  3.6  0.00  0.00  0.00  0.00     3
  9 AUG  105 75% Silkin    6285   3.27  24.1  110  1.8  0.01  0.03  0.00  0.00     4
 27 AUG  123 Beg Gr Fil   11849   3.13  24.1  114  1.0  0.00  0.05  0.00  0.00     5
  8 NOV  196 End Gr Fil   20195   0.64  24.1  132  0.7  0.00  0.44  0.00  0.00     6
 12 NOV  200 Maturity     20195   0.64  24.1  132  0.7  0.00  0.83  0.00  0.00    10
 12 NOV  200 Harvest      20195   0.64  24.1  132  0.7  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     105          -99
      Physiological maturity day (dap)                       200          -99
      Yield at harvest maturity (kg [dm]/ha)                8747          -99
      Number at maturity (no/m2)                            2000          -99
      Unit wt at maturity (g [dm]/unit)                   0.4374          -99
      Number at maturity (no/unit)                         500.0          -99
      Tops weight at maturity (kg [dm]/ha)                 20195          -99
      By-product produced (stalk) at maturity (kg[dm]/ha   11504          -99
      Leaf area index, maximum                              3.28          -99
      Harvest index at maturity                            0.433          -99
      Grain N at maturity (kg/ha)                            100          -99
      Tops N at maturity (kg/ha)                             132          -99
      Stem N at maturity (kg/ha)                              32          -99
      Grain N at maturity (%)                                1.1          -99
      Tops weight at anthesis (kg [dm]/ha)                  5994          -99
      Tops N at anthesis (kg/ha)                             110          -99
      Leaf number per stem at maturity                     24.12          -99
      Emergence day (dap)                                      8          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    28  21.8  13.6  17.7  20.9  12.05  380.0  182.4   65.9  138.6    0    0    0    0    0   16  0.000  0.000  0.002  0.005  0.000  0.000
 End Juvenil-Floral Init    5  23.4  13.3  18.3  21.9  12.06  380.0    0.1    8.7   25.2    0    0    0    0    0    1  0.000  0.000  0.000  0.001  0.000  0.000
 Floral Init-End Lf Grow   64  21.2  12.9  17.1  18.4  12.06  380.0  582.2  196.6  246.9    0    0    0    0    0   40  0.000  0.009  0.014  0.034  0.000  0.000
 End Lf Grth-Beg Grn Fil   18  21.6  12.7  17.2  21.6  12.03  380.0   65.6   78.9   79.4    0    0    0    0    0   13  0.000  0.000  0.021  0.052  0.000  0.000
 Grain Filling Phase       73  22.6  12.9  17.8  22.5  11.99  380.0  316.7  299.5  350.2    0    0    0    0    0   45  0.000  0.000  0.232  0.427  0.000  0.000
   
 Planting to Harvest      200  22.1  13.1  17.6  20.9  12.03  380.0   1184    663    907    0    0    0    0    0  120  0.000  0.003  0.103  0.189  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 200 days 

 Precipitation during growing season      1184.1 mm[rain]
   Dry Matter Productivity                  1.71 kg[DM]/m3[rain]          =   17.1 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.74 kg[grain yield]/m3[rain] =    7.4 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  662.7 mm[ET]
   Dry Matter Productivity                  3.05 kg[DM]/m3[ET]            =   30.5 kg[DM]/ha per mm[ET]
   Yield Productivity                       1.32 kg[grain yield]/m3[ET]   =   13.2 kg[yield]/ha per mm[ET]

 Transpiration during growing season       512.2 mm[EP]
   Dry Matter Productivity                  3.94 kg[DM]/m3[EP]            =   39.4 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.71 kg[grain yield]/m3[EP]   =   17.1 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                 134.6 kg[DM]/kg[N applied]
   Yield Productivity                       58.3 kg[yield]/kg[N applied]

 N uptake during growing season              219 kg[N uptake]/ha
   Dry Matter Productivity                  92.2 kg[DM]/kg[N uptake]
   Yield Productivity                       39.9 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     8747 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:39

*RUN  27        : SAMPLE 27                 MZCER048 EXPEFILE   27
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 27   : SAMPLE 27                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEBA   2021
 SOIL           : IB00000016     TEXTURE : ClayL -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:246.4mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.207 0.330 0.424 0.123 0.330   1.00   1.18   5.85   0.00   0.00   2.55
  5- 15 0.225 0.350 0.434 0.125 0.350   0.85   1.21   5.92   0.00   0.00   2.16
 15- 30 0.226 0.350 0.433 0.124 0.350   0.70   1.22   5.88   0.00   0.00   1.58
 30- 45 0.247 0.372 0.446 0.125 0.372   0.50   1.28   5.99   0.00   0.00   1.01
 45- 60 0.247 0.372 0.446 0.125 0.372   0.50   1.28   5.99   0.00   0.00   1.01
 60- 80 0.247 0.371 0.445 0.124 0.371   0.38   1.35   6.13   0.00   0.00   0.59
 80-100 0.247 0.371 0.445 0.124 0.371   0.38   1.35   6.13   0.00   0.00   0.59
100-150 0.232 0.354 0.434 0.122 0.354   0.05   1.42   6.30   0.00   0.00   0.33
150-200 0.232 0.354 0.434 0.122 0.354   0.05   1.42   6.30   0.00   0.00   0.33

TOT-200  47.2  71.8  87.5  24.6  71.8  <--cm   -  kg/ha-->    0.0    0.0 187599
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    27     SAMPLE 27               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 27 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 28 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  7 MAY   10 Emergence       16   0.00   1.8    1  4.4  0.00  0.00  0.00  0.00     1
 15 JUN   49 End Juveni     222   0.40  10.1    9  3.9  0.00  0.01  0.00  0.00     2
 20 JUN   54 Floral Ini     314   0.53  11.0   11  3.6  0.00  0.00  0.00  0.00     3
 18 SEP  144 75% Silkin    3520   0.96  23.0   37  1.0  0.00  0.23  0.00  0.00     4
 11 OCT  167 Beg Gr Fil    5081   0.48  23.0   37  0.7  0.00  0.60  0.00  0.00     5
 11 JAN  259 End Gr Fil    6927   0.05  23.0   54  0.8  0.00  0.49  0.00  0.00     6
 16 JAN  264 Maturity      6927   0.05  23.0   54  0.8  0.00  0.16  0.00  0.00    10
 16 JAN  264 Harvest       6927   0.05  23.0   54  0.8  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     144          -99
      Physiological maturity day (dap)                       264          -99
      Yield at harvest maturity (kg [dm]/ha)                2163          -99
      Number at maturity (no/m2)                             400          -99
      Unit wt at maturity (g [dm]/unit)                   0.5403          -99
      Number at maturity (no/unit)                         100.1          -99
      Tops weight at maturity (kg [dm]/ha)                  6927          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    4803          -99
      Leaf area index, maximum                              1.64          -99
      Harvest index at maturity                            0.312          -99
      Grain N at maturity (kg/ha)                             23          -99
      Tops N at maturity (kg/ha)                              54          -99
      Stem N at maturity (kg/ha)                              31          -99
      Grain N at maturity (%)                                1.1          -99
      Tops weight at anthesis (kg [dm]/ha)                  3478          -99
      Tops N at anthesis (kg/ha)                              37          -99
      Leaf number per stem at maturity                     23.01          -99
      Emergence day (dap)                                     10          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    39  19.6  10.1  14.9  19.4  12.06  380.0  518.4  106.0  169.3    0    0    0    0    0   34  0.000  0.000  0.002  0.006  0.000  0.000
 End Juvenil-Floral Init    5  18.4  10.4  14.4  17.0  12.06  380.0   87.6   18.1   18.2    0    0    0    0    0    5  0.000  0.000  0.000  0.000  0.000  0.000
 Floral Init-End Lf Grow   90  18.4   9.9  14.2  17.7  12.04  380.0   3431    317    322    0    0    0    0    0   88  0.000  0.000  0.089  0.219  0.000  0.000
 End Lf Grth-Beg Grn Fil   23  19.9  10.4  15.2  19.4  11.99  380.0   1211     96     96    0    0    0    0    0   23  0.000  0.000  0.315  0.596  0.000  0.000
 Grain Filling Phase       92  21.4  10.1  15.8  21.3  11.95  380.0  506.7  259.8  444.6    0    0    0    0    0   70  0.000  0.000  0.230  0.491  0.000  0.000
   
 Planting to Harvest      264  20.0  10.1  15.0  19.6  12.01  380.0   6268    832   1127    0    0    0    0    0  231  0.000  0.000  0.140  0.302  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 264 days 

 Precipitation during growing season      6267.6 mm[rain]
   Dry Matter Productivity                  0.11 kg[DM]/m3[rain]          =    1.1 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.03 kg[grain yield]/m3[rain] =    0.3 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  832.2 mm[ET]
   Dry Matter Productivity                  0.83 kg[DM]/m3[ET]            =    8.3 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.26 kg[grain yield]/m3[ET]   =    2.6 kg[yield]/ha per mm[ET]

 Transpiration during growing season       280.5 mm[EP]
   Dry Matter Productivity                  2.47 kg[DM]/m3[EP]            =   24.7 kg[DM]/ha per mm[EP]
   Yield Productivity                       0.77 kg[grain yield]/m3[EP]   =    7.7 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                  46.2 kg[DM]/kg[N applied]
   Yield Productivity                       14.4 kg[yield]/kg[N applied]

 N uptake during growing season              118 kg[N uptake]/ha
   Dry Matter Productivity                  58.7 kg[DM]/kg[N uptake]
   Yield Productivity                       18.3 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     2163 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:39

*RUN  28        : SAMPLE 28                 MZCER048 EXPEFILE   28
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 28   : SAMPLE 28                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEBB   2021
 SOIL           : IB00000001     TEXTURE : yLoam -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:252.5mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.211 0.339 0.430 0.128 0.339   1.00   1.21   6.19   0.00   0.00   1.75
  5- 15 0.224 0.351 0.436 0.127 0.351   0.85   1.23   6.26   0.00   0.00   1.49
 15- 30 0.226 0.354 0.438 0.128 0.354   0.70   1.24   6.27   0.00   0.00   1.08
 30- 45 0.241 0.369 0.446 0.128 0.369   0.50   1.29   6.38   0.00   0.00   0.70
 45- 60 0.241 0.369 0.446 0.128 0.369   0.50   1.29   6.38   0.00   0.00   0.70
 60- 80 0.241 0.368 0.444 0.127 0.368   0.38   1.35   6.52   0.00   0.00   0.40
 80-100 0.241 0.368 0.444 0.127 0.368   0.38   1.35   6.52   0.00   0.00   0.40
100-150 0.231 0.356 0.437 0.125 0.356   0.05   1.41   6.69   0.00   0.00   0.23
150-200 0.231 0.356 0.437 0.125 0.356   0.05   1.41   6.69   0.00   0.00   0.23

TOT-200  46.7  71.9  87.9  25.3  71.9  <--cm   -  kg/ha-->    0.0    0.0 130123
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    28     SAMPLE 28               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 27 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 28 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  7 MAY   10 Emergence       16   0.00   1.8    1  4.4  0.00  0.00  0.00  0.00     1
 15 JUN   49 End Juveni     222   0.40  10.1    9  3.9  0.00  0.01  0.00  0.00     2
 20 JUN   54 Floral Ini     315   0.53  11.0   11  3.6  0.00  0.00  0.00  0.00     3
 18 SEP  144 75% Silkin    3730   0.86  23.0   38  1.0  0.00  0.28  0.00  0.00     4
 11 OCT  167 Beg Gr Fil    4933   0.41  23.0   37  0.8  0.00  0.62  0.00  0.00     5
 11 JAN  259 End Gr Fil    6437   0.03  23.0   46  0.7  0.00  0.54  0.00  0.00     6
 16 JAN  264 Maturity      6437   0.03  23.0   46  0.7  0.00  0.34  0.00  0.00    10
 16 JAN  264 Harvest       6437   0.03  23.0   46  0.7  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     144          -99
      Physiological maturity day (dap)                       264          -99
      Yield at harvest maturity (kg [dm]/ha)                1911          -99
      Number at maturity (no/m2)                             354          -99
      Unit wt at maturity (g [dm]/unit)                   0.5405          -99
      Number at maturity (no/unit)                          88.4          -99
      Tops weight at maturity (kg [dm]/ha)                  6437          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    4568          -99
      Leaf area index, maximum                              1.68          -99
      Harvest index at maturity                            0.297          -99
      Grain N at maturity (kg/ha)                             19          -99
      Tops N at maturity (kg/ha)                              46          -99
      Stem N at maturity (kg/ha)                              27          -99
      Grain N at maturity (%)                                1.0          -99
      Tops weight at anthesis (kg [dm]/ha)                  3698          -99
      Tops N at anthesis (kg/ha)                              38          -99
      Leaf number per stem at maturity                     23.01          -99
      Emergence day (dap)                                     10          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    39  19.6  10.1  14.9  19.4  12.06  380.0  518.4  106.3  169.4    0    0    0    0    0   34  0.000  0.000  0.002  0.006  0.000  0.000
 End Juvenil-Floral Init    5  18.4  10.4  14.4  17.0  12.06  380.0   87.6   18.2   18.2    0    0    0    0    0    5  0.000  0.000  0.000  0.000  0.000  0.000
 Floral Init-End Lf Grow   90  18.4   9.9  14.2  17.7  12.04  380.0   3431    317    322    0    0    0    0    0   88  0.000  0.000  0.124  0.273  0.000  0.000
 End Lf Grth-Beg Grn Fil   23  19.9  10.4  15.2  19.4  11.99  380.0   1211     96     97    0    0    0    0    0   23  0.000  0.000  0.347  0.622  0.000  0.000
 Grain Filling Phase       92  21.4  10.1  15.8  21.3  11.95  380.0  506.7  255.0  446.1    0    0    0    0    0   70  0.000  0.000  0.272  0.547  0.000  0.000
   
 Planting to Harvest      264  20.0  10.1  15.0  19.6  12.01  380.0   6268    829   1129    0    0    0    0    0  231  0.000  0.000  0.170  0.345  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 264 days 

 Precipitation during growing season      6267.6 mm[rain]
   Dry Matter Productivity                  0.10 kg[DM]/m3[rain]          =    1.0 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.03 kg[grain yield]/m3[rain] =    0.3 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  829.0 mm[ET]
   Dry Matter Productivity                  0.78 kg[DM]/m3[ET]            =    7.8 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.23 kg[grain yield]/m3[ET]   =    2.3 kg[yield]/ha per mm[ET]

 Transpiration during growing season       270.7 mm[EP]
   Dry Matter Productivity                  2.38 kg[DM]/m3[EP]            =   23.8 kg[DM]/ha per mm[EP]
   Yield Productivity                       0.71 kg[grain yield]/m3[EP]   =    7.1 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                  42.9 kg[DM]/kg[N applied]
   Yield Productivity                       12.7 kg[yield]/kg[N applied]

 N uptake during growing season               99 kg[N uptake]/ha
   Dry Matter Productivity                  65.0 kg[DM]/kg[N uptake]
   Yield Productivity                       19.3 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     1911 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:39

*RUN  29        : SAMPLE 29                 MZCER048 EXPEFILE   29
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 29   : SAMPLE 29                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEBC   2021
 SOIL           : IB00000011     TEXTURE : yLoam -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:247.6mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.217 0.342 0.430 0.125 0.342   1.00   1.20   5.99   0.00   0.00   2.16
  5- 15 0.229 0.354 0.436 0.125 0.354   0.85   1.22   6.06   0.00   0.00   1.83
 15- 30 0.231 0.356 0.437 0.125 0.356   0.70   1.23   6.02   0.00   0.00   1.32
 30- 45 0.243 0.368 0.443 0.125 0.368   0.50   1.28   6.15   0.00   0.00   0.85
 45- 60 0.243 0.368 0.443 0.125 0.368   0.50   1.28   6.15   0.00   0.00   0.85
 60- 80 0.244 0.368 0.443 0.124 0.368   0.38   1.35   6.31   0.00   0.00   0.49
 80-100 0.244 0.368 0.443 0.124 0.368   0.38   1.35   6.31   0.00   0.00   0.49
100-150 0.235 0.358 0.436 0.123 0.358   0.05   1.41   6.51   0.00   0.00   0.28
150-200 0.235 0.358 0.436 0.123 0.358   0.05   1.41   6.51   0.00   0.00   0.28

TOT-200  47.4  72.2  87.7  24.8  72.2  <--cm   -  kg/ha-->    0.0    0.0 158220
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    29     SAMPLE 29               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  5 MAY    9 Emergence       16   0.00   1.8    1  4.4  0.00  0.00  0.00  0.00     1
 10 JUN   45 End Juveni     226   0.41  10.1    9  3.8  0.00  0.01  0.00  0.00     2
 15 JUN   50 Floral Ini     346   0.57  11.2   13  3.6  0.00  0.00  0.00  0.00     3
  7 SEP  134 75% Silkin    5352   1.88  23.5   68  1.3  0.00  0.14  0.00  0.00     4
  1 OCT  158 Beg Gr Fil    9025   1.02  23.5   68  0.8  0.00  0.50  0.00  0.00     5
 28 DEC  246 End Gr Fil   12362   0.08  23.5   77  0.6  0.00  0.59  0.00  0.00     6
  3 JAN  252 Maturity     12362   0.08  23.5   77  0.6  0.00  0.58  0.00  0.00    10
  3 JAN  252 Harvest      12362   0.08  23.5   77  0.6  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     134          -99
      Physiological maturity day (dap)                       252          -99
      Yield at harvest maturity (kg [dm]/ha)                4080          -99
      Number at maturity (no/m2)                             780          -99
      Unit wt at maturity (g [dm]/unit)                   0.5229          -99
      Number at maturity (no/unit)                         195.1          -99
      Tops weight at maturity (kg [dm]/ha)                 12362          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    8335          -99
      Leaf area index, maximum                              2.63          -99
      Harvest index at maturity                            0.330          -99
      Grain N at maturity (kg/ha)                             38          -99
      Tops N at maturity (kg/ha)                              77          -99
      Stem N at maturity (kg/ha)                              39          -99
      Grain N at maturity (%)                                0.9          -99
      Tops weight at anthesis (kg [dm]/ha)                  5128          -99
      Tops N at anthesis (kg/ha)                              68          -99
      Leaf number per stem at maturity                     23.45          -99
      Emergence day (dap)                                      9          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    36  19.7  11.4  15.5  20.8  12.05  380.0  367.2   99.9  169.6    0    0    0    0    0   27  0.000  0.000  0.003  0.006  0.000  0.000
 End Juvenil-Floral Init    5  20.5  11.1  15.8  22.1  12.05  380.0    3.0    8.9   24.2    0    0    0    0    0    4  0.000  0.000  0.000  0.000  0.000  0.000
 Floral Init-End Lf Grow   84  18.7  10.7  14.7  19.0  12.04  380.0   1654    307    321    0    0    0    0    0   81  0.000  0.000  0.054  0.136  0.000  0.000
 End Lf Grth-Beg Grn Fil   24  19.5  10.8  15.2  21.3  12.00  380.0  469.0  104.4  104.9    0    0    0    0    0   24  0.000  0.000  0.216  0.497  0.000  0.000
 Grain Filling Phase       88  20.7  11.3  16.0  22.8  11.96  380.0  691.4  232.0  445.6    0    0    0    0    0   70  0.000  0.000  0.304  0.587  0.000  0.000
   
 Planting to Harvest      252  19.8  11.1  15.5  21.1  12.01  380.0   3286    780   1143    0    0    0    0    0  216  0.000  0.000  0.152  0.312  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 252 days 

 Precipitation during growing season      3285.8 mm[rain]
   Dry Matter Productivity                  0.38 kg[DM]/m3[rain]          =    3.8 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.12 kg[grain yield]/m3[rain] =    1.2 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  780.4 mm[ET]
   Dry Matter Productivity                  1.58 kg[DM]/m3[ET]            =   15.8 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.52 kg[grain yield]/m3[ET]   =    5.2 kg[yield]/ha per mm[ET]

 Transpiration during growing season       395.3 mm[EP]
   Dry Matter Productivity                  3.13 kg[DM]/m3[EP]            =   31.3 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.03 kg[grain yield]/m3[EP]   =   10.3 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                  82.4 kg[DM]/kg[N applied]
   Yield Productivity                       27.2 kg[yield]/kg[N applied]

 N uptake during growing season              155 kg[N uptake]/ha
   Dry Matter Productivity                  79.8 kg[DM]/kg[N uptake]
   Yield Productivity                       26.3 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     4080 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:39

*RUN  30        : SAMPLE 30                 MZCER048 EXPEFILE   30
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 30   : SAMPLE 30                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEBD   2021
 SOIL           : IB00000012     TEXTURE : Loam  -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:252.0mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.215 0.341 0.430 0.126 0.341   1.00   1.13   5.53   0.00   0.00   3.44
  5- 15 0.233 0.359 0.440 0.126 0.359   0.85   1.15   5.60   0.00   0.00   2.91
 15- 30 0.237 0.365 0.443 0.128 0.365   0.70   1.17   5.57   0.00   0.00   2.20
 30- 45 0.259 0.386 0.456 0.127 0.386   0.50   1.22   5.67   0.00   0.00   1.41
 45- 60 0.259 0.386 0.456 0.127 0.386   0.50   1.22   5.67   0.00   0.00   1.41
 60- 80 0.258 0.385 0.455 0.127 0.385   0.38   1.29   5.79   0.00   0.00   0.81
 80-100 0.258 0.385 0.455 0.127 0.385   0.38   1.29   5.79   0.00   0.00   0.81
100-150 0.244 0.369 0.444 0.125 0.369   0.05   1.34   5.97   0.00   0.00   0.47
150-200 0.244 0.369 0.444 0.125 0.369   0.05   1.34   5.97   0.00   0.00   0.47

TOT-200  49.5  74.7  89.5  25.2  74.7  <--cm   -  kg/ha-->    0.0    0.0 247893
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    30     SAMPLE 30               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  2 MAY    6 Emergence       16   0.00   2.1    1  4.4  0.00  0.01  0.00  0.00     1
 26 MAY   30 End Juveni     274   0.47  10.6   10  3.6  0.00  0.00  0.00  0.00     2
 31 MAY   35 Floral Ini     516   0.79  12.3   19  3.6  0.00  0.00  0.00  0.00     3
 20 JUL   85 75% Silkin    5472   2.27  25.4   92  1.7  0.17  0.16  0.00  0.00     4
  5 AUG  101 Beg Gr Fil    8411   2.16  25.4   96  1.1  0.00  0.06  0.00  0.00     5
  3 OCT  160 End Gr Fil   13240   1.32  25.4  109  0.8  0.00  0.17  0.00  0.00     6
  7 OCT  164 Maturity     13240   1.32  25.4  109  0.8  0.00  0.50  0.00  0.00    10
  7 OCT  164 Harvest      13240   1.32  25.4  109  0.8  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                      85          -99
      Physiological maturity day (dap)                       164          -99
      Yield at harvest maturity (kg [dm]/ha)                4912          -99
      Number at maturity (no/m2)                            1388          -99
      Unit wt at maturity (g [dm]/unit)                   0.3540          -99
      Number at maturity (no/unit)                         346.9          -99
      Tops weight at maturity (kg [dm]/ha)                 13240          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    8383          -99
      Leaf area index, maximum                              2.31          -99
      Harvest index at maturity                            0.371          -99
      Grain N at maturity (kg/ha)                             73          -99
      Tops N at maturity (kg/ha)                             109          -99
      Stem N at maturity (kg/ha)                              36          -99
      Grain N at maturity (%)                                1.5          -99
      Tops weight at anthesis (kg [dm]/ha)                  5257          -99
      Tops N at anthesis (kg/ha)                              92          -99
      Leaf number per stem at maturity                     25.42          -99
      Emergence day (dap)                                      6          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    24  24.6  15.9  20.2  20.1  12.04  380.0  176.1   64.5  120.6    0    0    0    0    0   15  0.000  0.000  0.002  0.005  0.000  0.000
 End Juvenil-Floral Init    5  25.7  15.6  20.7  22.5  12.05  380.0    0.2   11.6   27.1    0    0    0    0    0    1  0.000  0.000  0.001  0.002  0.000  0.000
 Floral Init-End Lf Grow   50  25.1  15.1  20.1  19.1  12.05  380.0  203.0  146.5  214.2    0    0    0    0    0   23  0.112  0.167  0.063  0.158  0.000  0.000
 End Lf Grth-Beg Grn Fil   16  22.3  14.8  18.6  16.0  12.04  380.0  247.8   54.8   55.0    0    0    0    0    0   14  0.000  0.000  0.027  0.068  0.000  0.000
 Grain Filling Phase       59  25.2  14.8  20.0  21.7  12.02  380.0  186.5  275.5  285.0    0    0    0    0    0   42  0.000  0.000  0.065  0.163  0.000  0.000
   
 Planting to Harvest      164  24.9  15.1  20.0  20.2  12.03  380.0  818.4  576.8  758.9    0    0    0    0    0  100  0.034  0.051  0.051  0.127  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 164 days 

 Precipitation during growing season       818.4 mm[rain]
   Dry Matter Productivity                  1.62 kg[DM]/m3[rain]          =   16.2 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.60 kg[grain yield]/m3[rain] =    6.0 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  576.8 mm[ET]
   Dry Matter Productivity                  2.30 kg[DM]/m3[ET]            =   23.0 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.85 kg[grain yield]/m3[ET]   =    8.5 kg[yield]/ha per mm[ET]

 Transpiration during growing season       410.5 mm[EP]
   Dry Matter Productivity                  3.23 kg[DM]/m3[EP]            =   32.3 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.20 kg[grain yield]/m3[EP]   =   12.0 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                  88.3 kg[DM]/kg[N applied]
   Yield Productivity                       32.7 kg[yield]/kg[N applied]

 N uptake during growing season              173 kg[N uptake]/ha
   Dry Matter Productivity                  76.5 kg[DM]/kg[N uptake]
   Yield Productivity                       28.4 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     4912 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:39

*RUN  31        : SAMPLE 31                 MZCER048 EXPEFILE   31
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 31   : SAMPLE 31                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEBE   2021
 SOIL           : IB00000017     TEXTURE : yLoam -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:255.3mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.225 0.353 0.437 0.128 0.353   1.00   1.14   5.78   0.00   0.00   2.49
  5- 15 0.236 0.364 0.443 0.128 0.364   0.85   1.16   5.85   0.00   0.00   2.11
 15- 30 0.243 0.371 0.448 0.128 0.371   0.70   1.18   5.82   0.00   0.00   1.53
 30- 45 0.256 0.385 0.456 0.129 0.385   0.50   1.23   5.93   0.00   0.00   0.98
 45- 60 0.256 0.385 0.456 0.129 0.385   0.50   1.23   5.93   0.00   0.00   0.98
 60- 80 0.256 0.384 0.455 0.128 0.384   0.38   1.31   6.07   0.00   0.00   0.57
 80-100 0.256 0.384 0.455 0.128 0.384   0.38   1.31   6.07   0.00   0.00   0.57
100-150 0.247 0.374 0.448 0.127 0.374   0.05   1.36   6.25   0.00   0.00   0.32
150-200 0.247 0.374 0.448 0.127 0.374   0.05   1.36   6.25   0.00   0.00   0.32

TOT-200  49.8  75.3  90.0  25.5  75.3  <--cm   -  kg/ha-->    0.0    0.0 175300
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    31     SAMPLE 31               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  2 MAY    6 Emergence       16   0.00   2.1    1  4.4  0.00  0.01  0.00  0.00     1
 26 MAY   30 End Juveni     274   0.47  10.6   10  3.6  0.00  0.00  0.00  0.00     2
 31 MAY   35 Floral Ini     516   0.79  12.3   19  3.6  0.00  0.00  0.00  0.00     3
 20 JUL   85 75% Silkin    5344   2.14  25.4   89  1.7  0.17  0.19  0.00  0.00     4
  5 AUG  101 Beg Gr Fil    8130   1.98  25.4   91  1.1  0.00  0.09  0.00  0.00     5
  3 OCT  160 End Gr Fil   12771   1.09  25.4  102  0.8  0.00  0.21  0.00  0.00     6
  7 OCT  164 Maturity     12771   1.09  25.4  102  0.8  0.00  0.53  0.00  0.00    10
  7 OCT  164 Harvest      12771   1.09  25.4  102  0.8  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                      85          -99
      Physiological maturity day (dap)                       164          -99
      Yield at harvest maturity (kg [dm]/ha)                4721          -99
      Number at maturity (no/m2)                            1334          -99
      Unit wt at maturity (g [dm]/unit)                   0.3540          -99
      Number at maturity (no/unit)                         333.4          -99
      Tops weight at maturity (kg [dm]/ha)                 12771          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    8104          -99
      Leaf area index, maximum                              2.21          -99
      Harvest index at maturity                            0.370          -99
      Grain N at maturity (kg/ha)                             68          -99
      Tops N at maturity (kg/ha)                             102          -99
      Stem N at maturity (kg/ha)                              34          -99
      Grain N at maturity (%)                                1.4          -99
      Tops weight at anthesis (kg [dm]/ha)                  5138          -99
      Tops N at anthesis (kg/ha)                              88          -99
      Leaf number per stem at maturity                     25.42          -99
      Emergence day (dap)                                      6          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    24  24.6  15.9  20.2  20.1  12.04  380.0  176.1   64.6  120.7    0    0    0    0    0   15  0.000  0.000  0.002  0.005  0.000  0.000
 End Juvenil-Floral Init    5  25.7  15.6  20.7  22.5  12.05  380.0    0.2   11.5   27.1    0    0    0    0    0    1  0.000  0.000  0.001  0.002  0.000  0.000
 Floral Init-End Lf Grow   50  25.1  15.1  20.1  19.1  12.05  380.0  203.0  146.0  214.5    0    0    0    0    0   23  0.112  0.166  0.077  0.188  0.000  0.000
 End Lf Grth-Beg Grn Fil   16  22.3  14.8  18.6  16.0  12.04  380.0  247.8   55.0   55.3    0    0    0    0    0   14  0.000  0.000  0.039  0.098  0.000  0.000
 Grain Filling Phase       59  25.2  14.8  20.0  21.7  12.02  380.0  186.5  270.2  287.4    0    0    0    0    0   42  0.000  0.000  0.081  0.201  0.000  0.000
   
 Planting to Harvest      164  24.9  15.1  20.0  20.2  12.03  380.0  818.4  571.2  762.2    0    0    0    0    0  100  0.034  0.051  0.062  0.153  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 164 days 

 Precipitation during growing season       818.4 mm[rain]
   Dry Matter Productivity                  1.56 kg[DM]/m3[rain]          =   15.6 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.58 kg[grain yield]/m3[rain] =    5.8 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  571.2 mm[ET]
   Dry Matter Productivity                  2.24 kg[DM]/m3[ET]            =   22.4 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.83 kg[grain yield]/m3[ET]   =    8.3 kg[yield]/ha per mm[ET]

 Transpiration during growing season       393.6 mm[EP]
   Dry Matter Productivity                  3.24 kg[DM]/m3[EP]            =   32.4 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.20 kg[grain yield]/m3[EP]   =   12.0 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                  85.1 kg[DM]/kg[N applied]
   Yield Productivity                       31.5 kg[yield]/kg[N applied]

 N uptake during growing season              161 kg[N uptake]/ha
   Dry Matter Productivity                  79.3 kg[DM]/kg[N uptake]
   Yield Productivity                       29.3 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     4721 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:39

*RUN  32        : SAMPLE 32                 MZCER048 EXPEFILE   32
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 32   : SAMPLE 32                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEBF   2021
 SOIL           : IB00000002     TEXTURE : ClayL -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:248.4mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.204 0.329 0.424 0.125 0.329   1.00   1.24   6.34   0.00   0.00   1.54
  5- 15 0.218 0.344 0.431 0.126 0.344   0.85   1.26   6.41   0.00   0.00   1.30
 15- 30 0.218 0.343 0.431 0.125 0.343   0.70   1.27   6.44   0.00   0.00   0.93
 30- 45 0.233 0.359 0.439 0.126 0.359   0.50   1.32   6.55   0.00   0.00   0.59
 45- 60 0.233 0.359 0.439 0.126 0.359   0.50   1.32   6.55   0.00   0.00   0.59
 60- 80 0.233 0.358 0.438 0.125 0.358   0.38   1.37   6.68   0.00   0.00   0.34
 80-100 0.233 0.358 0.438 0.125 0.358   0.38   1.37   6.68   0.00   0.00   0.34
100-150 0.222 0.345 0.430 0.123 0.345   0.05   1.43   6.86   0.00   0.00   0.21
150-200 0.222 0.345 0.430 0.123 0.345   0.05   1.43   6.86   0.00   0.00   0.21

TOT-200  45.0  69.8  86.6  24.8  69.8  <--cm   -  kg/ha-->    0.0    0.0 115671
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    32     SAMPLE 32               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  6 MAY   10 Emergence       16   0.00   1.8    1  4.4  0.00  0.00  0.00  0.00     1
 16 JUN   51 End Juveni     229   0.41  10.2    9  3.9  0.00  0.01  0.00  0.00     2
 21 JUN   56 Floral Ini     329   0.55  11.1   12  3.6  0.00  0.00  0.00  0.00     3
 23 SEP  150 75% Silkin    5733   2.18  23.2   80  1.4  0.00  0.10  0.00  0.00     4
 18 OCT  175 Beg Gr Fil   10664   1.28  23.2   82  0.8  0.02  0.43  0.00  0.00     5
 25 JAN  274 End Gr Fil   13078   0.04  23.2   72  0.5  0.62  0.60  0.00  0.00     6
 31 JAN  280 Maturity     13078   0.04  23.2   72  0.5  0.00  0.75  0.00  0.00    10
 31 JAN  280 Harvest      13078   0.04  23.2   72  0.5  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     150          -99
      Physiological maturity day (dap)                       280          -99
      Yield at harvest maturity (kg [dm]/ha)                4156          -99
      Number at maturity (no/m2)                            1031          -99
      Unit wt at maturity (g [dm]/unit)                   0.4032          -99
      Number at maturity (no/unit)                         257.7          -99
      Tops weight at maturity (kg [dm]/ha)                 13078          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    8976          -99
      Leaf area index, maximum                              2.71          -99
      Harvest index at maturity                            0.318          -99
      Grain N at maturity (kg/ha)                             41          -99
      Tops N at maturity (kg/ha)                              72          -99
      Stem N at maturity (kg/ha)                              31          -99
      Grain N at maturity (%)                                1.0          -99
      Tops weight at anthesis (kg [dm]/ha)                  5444          -99
      Tops N at anthesis (kg/ha)                              79          -99
      Leaf number per stem at maturity                     23.23          -99
      Emergence day (dap)                                     10          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    41  18.9  10.3  14.6  20.6  12.01  380.0  151.5   70.8  185.8    0    0    0    0    0   14  0.000  0.000  0.003  0.007  0.000  0.000
 End Juvenil-Floral Init    5  18.7  10.2  14.4  17.9  12.01  380.0    0.4    6.1   18.8    0    0    0    0    0    2  0.000  0.000  0.000  0.001  0.000  0.000
 Floral Init-End Lf Grow   94  18.0   9.8  13.9  18.4  12.01  380.0  519.3  304.1  339.7    0    0    0    0    0   75  0.000  0.000  0.037  0.092  0.000  0.000
 End Lf Grth-Beg Grn Fil   25  19.5  10.1  14.8  22.2  12.00  380.0   25.2  107.5  110.8    0    0    0    0    0   24  0.000  0.013  0.167  0.417  0.000  0.000
 Grain Filling Phase       99  20.3  10.2  15.2  22.5  11.99  380.0   47.3   79.6  484.5    0    0    0    0    0   45  0.489  0.627  0.321  0.601  0.000  0.000
   
 Planting to Harvest      280  19.2  10.1  14.7  20.8  12.00  380.0  779.9  593.4 1224.0    0    0    0    0    0  167  0.173  0.223  0.152  0.298  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 280 days 

 Precipitation during growing season       779.9 mm[rain]
   Dry Matter Productivity                  1.68 kg[DM]/m3[rain]          =   16.8 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.53 kg[grain yield]/m3[rain] =    5.3 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  593.4 mm[ET]
   Dry Matter Productivity                  2.20 kg[DM]/m3[ET]            =   22.0 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.70 kg[grain yield]/m3[ET]   =    7.0 kg[yield]/ha per mm[ET]

 Transpiration during growing season       367.6 mm[EP]
   Dry Matter Productivity                  3.56 kg[DM]/m3[EP]            =   35.6 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.13 kg[grain yield]/m3[EP]   =   11.3 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                  87.2 kg[DM]/kg[N applied]
   Yield Productivity                       27.7 kg[yield]/kg[N applied]

 N uptake during growing season              158 kg[N uptake]/ha
   Dry Matter Productivity                  82.8 kg[DM]/kg[N uptake]
   Yield Productivity                       26.3 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     4156 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:39

*RUN  33        : SAMPLE 33                 MZCER048 EXPEFILE   33
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 33   : SAMPLE 33                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEBG   2021
 SOIL           : IB00000009     TEXTURE : yLoam -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:253.8mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.217 0.343 0.432 0.126 0.343   1.00   1.14   6.01   0.00   0.00   2.35
  5- 15 0.229 0.356 0.438 0.127 0.356   0.85   1.17   6.08   0.00   0.00   1.99
 15- 30 0.233 0.361 0.441 0.128 0.361   0.70   1.18   6.07   0.00   0.00   1.44
 30- 45 0.246 0.374 0.449 0.128 0.374   0.50   1.23   6.18   0.00   0.00   0.92
 45- 60 0.246 0.374 0.449 0.128 0.374   0.50   1.23   6.18   0.00   0.00   0.92
 60- 80 0.246 0.374 0.448 0.128 0.374   0.38   1.31   6.31   0.00   0.00   0.54
 80-100 0.246 0.374 0.448 0.128 0.374   0.38   1.31   6.31   0.00   0.00   0.54
100-150 0.237 0.363 0.441 0.126 0.363   0.05   1.36   6.50   0.00   0.00   0.31
150-200 0.237 0.363 0.441 0.126 0.363   0.05   1.36   6.50   0.00   0.00   0.31

TOT-200  47.8  73.2  88.6  25.4  73.2  <--cm   -  kg/ha-->    0.0    0.0 166570
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    33     SAMPLE 33               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  4 MAY    8 Emergence       16   0.00   1.9    1  4.4  0.00  0.00  0.00  0.00     1
  1 JUN   36 End Juveni     222   0.40  10.0    8  3.7  0.00  0.00  0.00  0.00     2
  6 JUN   41 Floral Ini     387   0.62  11.5   14  3.6  0.00  0.00  0.00  0.00     3
  9 AUG  105 75% Silkin    6340   3.22  24.1  110  1.7  0.00  0.04  0.00  0.00     4
 27 AUG  123 Beg Gr Fil   11753   2.98  24.1  112  1.0  0.00  0.09  0.00  0.00     5
  8 NOV  196 End Gr Fil   19845   0.51  24.1  126  0.6  0.00  0.49  0.00  0.00     6
 12 NOV  200 Maturity     19845   0.51  24.1  126  0.6  0.00  0.84  0.00  0.00    10
 12 NOV  200 Harvest      19845   0.51  24.1  126  0.6  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     105          -99
      Physiological maturity day (dap)                       200          -99
      Yield at harvest maturity (kg [dm]/ha)                8747          -99
      Number at maturity (no/m2)                            2000          -99
      Unit wt at maturity (g [dm]/unit)                   0.4374          -99
      Number at maturity (no/unit)                         500.0          -99
      Tops weight at maturity (kg [dm]/ha)                 19845          -99
      By-product produced (stalk) at maturity (kg[dm]/ha   11155          -99
      Leaf area index, maximum                              3.23          -99
      Harvest index at maturity                            0.441          -99
      Grain N at maturity (kg/ha)                             95          -99
      Tops N at maturity (kg/ha)                             126          -99
      Stem N at maturity (kg/ha)                              31          -99
      Grain N at maturity (%)                                1.1          -99
      Tops weight at anthesis (kg [dm]/ha)                  6053          -99
      Tops N at anthesis (kg/ha)                             109          -99
      Leaf number per stem at maturity                     24.12          -99
      Emergence day (dap)                                      8          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    28  21.8  13.6  17.7  20.9  12.05  380.0  182.4   66.3  138.6    0    0    0    0    0   16  0.000  0.000  0.002  0.005  0.000  0.000
 End Juvenil-Floral Init    5  23.4  13.3  18.3  21.9  12.06  380.0    0.1    8.9   25.3    0    0    0    0    0    1  0.000  0.000  0.000  0.001  0.000  0.000
 Floral Init-End Lf Grow   64  21.2  12.9  17.1  18.4  12.06  380.0  582.2  196.5  247.1    0    0    0    0    0   40  0.000  0.004  0.017  0.043  0.000  0.000
 End Lf Grth-Beg Grn Fil   18  21.6  12.7  17.2  21.6  12.03  380.0   65.6   79.0   79.5    0    0    0    0    0   13  0.000  0.000  0.035  0.088  0.000  0.000
 Grain Filling Phase       73  22.6  12.9  17.8  22.5  11.99  380.0  316.7  293.0  353.0    0    0    0    0    0   45  0.000  0.000  0.265  0.475  0.000  0.000
   
 Planting to Harvest      200  22.1  13.1  17.6  20.9  12.03  380.0   1184    656    910    0    0    0    0    0  120  0.000  0.001  0.118  0.213  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 200 days 

 Precipitation during growing season      1184.1 mm[rain]
   Dry Matter Productivity                  1.68 kg[DM]/m3[rain]          =   16.8 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.74 kg[grain yield]/m3[rain] =    7.4 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  655.6 mm[ET]
   Dry Matter Productivity                  3.03 kg[DM]/m3[ET]            =   30.3 kg[DM]/ha per mm[ET]
   Yield Productivity                       1.33 kg[grain yield]/m3[ET]   =   13.3 kg[yield]/ha per mm[ET]

 Transpiration during growing season       494.3 mm[EP]
   Dry Matter Productivity                  4.02 kg[DM]/m3[EP]            =   40.2 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.77 kg[grain yield]/m3[EP]   =   17.7 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                 132.3 kg[DM]/kg[N applied]
   Yield Productivity                       58.3 kg[yield]/kg[N applied]

 N uptake during growing season              205 kg[N uptake]/ha
   Dry Matter Productivity                  96.8 kg[DM]/kg[N uptake]
   Yield Productivity                       42.7 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     8747 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:39

*RUN  34        : SAMPLE 34                 MZCER048 EXPEFILE   34
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 34   : SAMPLE 34                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEBH   2021
 SOIL           : IB00000004     TEXTURE : ClayL -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:244.6mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.202 0.326 0.422 0.124 0.326   1.00   1.25   6.35   0.00   0.00   1.56
  5- 15 0.216 0.340 0.429 0.124 0.340   0.85   1.27   6.43   0.00   0.00   1.31
 15- 30 0.216 0.340 0.428 0.124 0.340   0.70   1.28   6.43   0.00   0.00   0.93
 30- 45 0.231 0.355 0.436 0.124 0.355   0.50   1.33   6.53   0.00   0.00   0.60
 45- 60 0.231 0.355 0.436 0.124 0.355   0.50   1.33   6.53   0.00   0.00   0.60
 60- 80 0.232 0.355 0.435 0.123 0.355   0.38   1.39   6.66   0.00   0.00   0.35
 80-100 0.232 0.355 0.435 0.123 0.355   0.38   1.39   6.66   0.00   0.00   0.35
100-150 0.220 0.341 0.428 0.121 0.341   0.05   1.43   6.85   0.00   0.00   0.20
150-200 0.220 0.341 0.428 0.121 0.341   0.05   1.43   6.85   0.00   0.00   0.20

TOT-200  44.6  69.1  86.1  24.5  69.1  <--cm   -  kg/ha-->    0.0    0.0 116243
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    34     SAMPLE 34               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  5 MAY    9 Emergence       16   0.00   1.9    1  4.4  0.00  0.00  0.00  0.00     1
  8 JUN   43 End Juveni     219   0.40  10.0    8  3.8  0.00  0.00  0.00  0.00     2
 13 JUN   48 Floral Ini     346   0.57  11.2   13  3.6  0.00  0.00  0.00  0.00     3
  1 SEP  128 75% Silkin    5199   1.79  23.5   66  1.3  0.00  0.15  0.00  0.00     4
 24 SEP  151 Beg Gr Fil    8395   0.98  23.5   65  0.8  0.00  0.51  0.00  0.00     5
 15 DEC  233 End Gr Fil   11408   0.08  23.5   70  0.6  0.00  0.60  0.00  0.00     6
 20 DEC  238 Maturity     11408   0.08  23.5   70  0.6  0.00  0.60  0.00  0.00    10
 20 DEC  238 Harvest      11408   0.08  23.5   70  0.6  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     128          -99
      Physiological maturity day (dap)                       238          -99
      Yield at harvest maturity (kg [dm]/ha)                3529          -99
      Number at maturity (no/m2)                             714          -99
      Unit wt at maturity (g [dm]/unit)                   0.4944          -99
      Number at maturity (no/unit)                         178.5          -99
      Tops weight at maturity (kg [dm]/ha)                 11408          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    7930          -99
      Leaf area index, maximum                              2.48          -99
      Harvest index at maturity                            0.309          -99
      Grain N at maturity (kg/ha)                             33          -99
      Tops N at maturity (kg/ha)                              70          -99
      Stem N at maturity (kg/ha)                              37          -99
      Grain N at maturity (%)                                0.9          -99
      Tops weight at anthesis (kg [dm]/ha)                  5021          -99
      Tops N at anthesis (kg/ha)                              66          -99
      Leaf number per stem at maturity                     23.49          -99
      Emergence day (dap)                                      9          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    34  20.1  11.8  15.9  20.4  12.05  380.0  367.2   95.4  158.5    0    0    0    0    0   27  0.000  0.000  0.002  0.006  0.000  0.000
 End Juvenil-Floral Init    5  20.9  11.2  16.0  22.6  12.05  380.0    1.2    8.9   24.9    0    0    0    0    0    2  0.000  0.000  0.000  0.000  0.000  0.000
 Floral Init-End Lf Grow   80  19.1  11.0  15.0  18.9  12.04  380.0   1419    290    306    0    0    0    0    0   77  0.000  0.000  0.060  0.149  0.000  0.000
 End Lf Grth-Beg Grn Fil   23  19.7  11.3  15.5  20.2  12.01  380.0  592.8   95.8   96.2    0    0    0    0    0   23  0.000  0.000  0.224  0.507  0.000  0.000
 Grain Filling Phase       82  21.3  12.1  16.7  22.8  11.97  380.0  794.1  236.9  421.6    0    0    0    0    0   67  0.000  0.000  0.319  0.599  0.000  0.000
   
 Planting to Harvest      238  20.3  11.6  15.9  20.9  12.01  380.0   3275    756   1082    0    0    0    0    0  205  0.000  0.000  0.158  0.319  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 238 days 

 Precipitation during growing season      3274.5 mm[rain]
   Dry Matter Productivity                  0.35 kg[DM]/m3[rain]          =    3.5 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.11 kg[grain yield]/m3[rain] =    1.1 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  756.4 mm[ET]
   Dry Matter Productivity                  1.51 kg[DM]/m3[ET]            =   15.1 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.47 kg[grain yield]/m3[ET]   =    4.7 kg[yield]/ha per mm[ET]

 Transpiration during growing season       371.1 mm[EP]
   Dry Matter Productivity                  3.07 kg[DM]/m3[EP]            =   30.7 kg[DM]/ha per mm[EP]
   Yield Productivity                       0.95 kg[grain yield]/m3[EP]   =    9.5 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                  76.1 kg[DM]/kg[N applied]
   Yield Productivity                       23.5 kg[yield]/kg[N applied]

 N uptake during growing season              139 kg[N uptake]/ha
   Dry Matter Productivity                  82.1 kg[DM]/kg[N uptake]
   Yield Productivity                       25.4 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     3529 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:40

*RUN  35        : SAMPLE 35                 MZCER048 EXPEFILE   35
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 35   : SAMPLE 35                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEBI   2021
 SOIL           : IB00000018     TEXTURE : yLoam -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:253.6mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.213 0.341 0.431 0.128 0.341   1.00   1.21   6.12   0.00   0.00   1.86
  5- 15 0.225 0.353 0.437 0.128 0.353   0.85   1.23   6.20   0.00   0.00   1.58
 15- 30 0.229 0.357 0.439 0.128 0.357   0.70   1.23   6.20   0.00   0.00   1.13
 30- 45 0.243 0.371 0.447 0.128 0.371   0.50   1.28   6.31   0.00   0.00   0.73
 45- 60 0.243 0.371 0.447 0.128 0.371   0.50   1.28   6.31   0.00   0.00   0.73
 60- 80 0.243 0.370 0.446 0.127 0.370   0.38   1.35   6.44   0.00   0.00   0.42
 80-100 0.243 0.370 0.446 0.127 0.370   0.38   1.35   6.44   0.00   0.00   0.42
100-150 0.233 0.359 0.439 0.126 0.359   0.05   1.40   6.61   0.00   0.00   0.24
150-200 0.233 0.359 0.439 0.126 0.359   0.05   1.40   6.61   0.00   0.00   0.24

TOT-200  47.1  72.4  88.3  25.4  72.4  <--cm   -  kg/ha-->    0.0    0.0 135848
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    35     SAMPLE 35               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  4 MAY    8 Emergence       16   0.00   1.9    1  4.4  0.00  0.00  0.00  0.00     1
  3 JUN   38 End Juveni     227   0.41  10.1    8  3.7  0.00  0.00  0.00  0.00     2
  8 JUN   43 Floral Ini     381   0.62  11.5   14  3.6  0.00  0.00  0.00  0.00     3
 15 AUG  111 75% Silkin    6288   3.20  23.8  105  1.7  0.00  0.04  0.00  0.00     4
  3 SEP  130 Beg Gr Fil   11871   2.74  23.8  107  0.9  0.00  0.17  0.00  0.00     5
 18 NOV  206 End Gr Fil   19516   0.33  23.8  117  0.6  0.00  0.56  0.00  0.00     6
 23 NOV  211 Maturity     19516   0.33  23.8  117  0.6  0.00  0.86  0.00  0.00    10
 23 NOV  211 Harvest      19516   0.33  23.8  117  0.6  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     111          -99
      Physiological maturity day (dap)                       211          -99
      Yield at harvest maturity (kg [dm]/ha)                8921          -99
      Number at maturity (no/m2)                            1961          -99
      Unit wt at maturity (g [dm]/unit)                   0.4549          -99
      Number at maturity (no/unit)                         490.3          -99
      Tops weight at maturity (kg [dm]/ha)                 19516          -99
      By-product produced (stalk) at maturity (kg[dm]/ha   10652          -99
      Leaf area index, maximum                              3.22          -99
      Harvest index at maturity                            0.457          -99
      Grain N at maturity (kg/ha)                             87          -99
      Tops N at maturity (kg/ha)                             117          -99
      Stem N at maturity (kg/ha)                              30          -99
      Grain N at maturity (%)                                1.0          -99
      Tops weight at anthesis (kg [dm]/ha)                  6021          -99
      Tops N at anthesis (kg/ha)                             105          -99
      Leaf number per stem at maturity                     23.82          -99
      Emergence day (dap)                                      8          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    30  20.9  13.4  17.2  21.0  12.06  380.0  182.4   68.3  147.1    0    0    0    0    0   16  0.000  0.000  0.002  0.006  0.000  0.000
 End Juvenil-Floral Init    5  22.4  13.2  17.8  21.8  12.07  380.0    0.1    8.2   24.8    0    0    0    0    0    1  0.000  0.000  0.000  0.000  0.000  0.000
 Floral Init-End Lf Grow   68  20.2  12.7  16.4  18.4  12.06  380.0  644.0  209.9  258.7    0    0    0    0    0   46  0.000  0.001  0.015  0.039  0.000  0.000
 End Lf Grth-Beg Grn Fil   19  21.1  12.5  16.8  21.9  12.03  380.0   60.3   84.1   84.5    0    0    0    0    0   14  0.000  0.000  0.064  0.161  0.000  0.000
 Grain Filling Phase       76  21.8  12.9  17.3  22.6  11.98  380.0  260.5  271.1  370.7    0    0    0    0    0   40  0.000  0.000  0.318  0.553  0.000  0.000
   
 Planting to Harvest      211  21.2  12.9  17.0  21.0  12.02  380.0   1184    653    958    0    0    0    0    0  122  0.000  0.000  0.141  0.247  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 211 days 

 Precipitation during growing season      1184.4 mm[rain]
   Dry Matter Productivity                  1.65 kg[DM]/m3[rain]          =   16.5 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.75 kg[grain yield]/m3[rain] =    7.5 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  652.7 mm[ET]
   Dry Matter Productivity                  2.99 kg[DM]/m3[ET]            =   29.9 kg[DM]/ha per mm[ET]
   Yield Productivity                       1.37 kg[grain yield]/m3[ET]   =   13.7 kg[yield]/ha per mm[ET]

 Transpiration during growing season       483.7 mm[EP]
   Dry Matter Productivity                  4.03 kg[DM]/m3[EP]            =   40.3 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.84 kg[grain yield]/m3[EP]   =   18.4 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                 130.1 kg[DM]/kg[N applied]
   Yield Productivity                       59.5 kg[yield]/kg[N applied]

 N uptake during growing season              197 kg[N uptake]/ha
   Dry Matter Productivity                  99.1 kg[DM]/kg[N uptake]
   Yield Productivity                       45.3 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     8921 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:40

*RUN  36        : SAMPLE 36                 MZCER048 EXPEFILE   36
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 36   : SAMPLE 36                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEBJ   2021
 SOIL           : IB00000017     TEXTURE : yLoam -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:255.3mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.225 0.353 0.437 0.128 0.353   1.00   1.14   5.78   0.00   0.00   2.49
  5- 15 0.236 0.364 0.443 0.128 0.364   0.85   1.16   5.85   0.00   0.00   2.11
 15- 30 0.243 0.371 0.448 0.128 0.371   0.70   1.18   5.82   0.00   0.00   1.53
 30- 45 0.256 0.385 0.456 0.129 0.385   0.50   1.23   5.93   0.00   0.00   0.98
 45- 60 0.256 0.385 0.456 0.129 0.385   0.50   1.23   5.93   0.00   0.00   0.98
 60- 80 0.256 0.384 0.455 0.128 0.384   0.38   1.31   6.07   0.00   0.00   0.57
 80-100 0.256 0.384 0.455 0.128 0.384   0.38   1.31   6.07   0.00   0.00   0.57
100-150 0.247 0.374 0.448 0.127 0.374   0.05   1.36   6.25   0.00   0.00   0.32
150-200 0.247 0.374 0.448 0.127 0.374   0.05   1.36   6.25   0.00   0.00   0.32

TOT-200  49.8  75.3  90.0  25.5  75.3  <--cm   -  kg/ha-->    0.0    0.0 175300
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    36     SAMPLE 36               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 27 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 28 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  8 MAY   11 Emergence       16   0.00   1.8    1  4.4  0.00  0.00  0.00  0.00     1
 20 JUN   54 End Juveni     219   0.39  10.1    9  3.9  0.00  0.00  0.00  0.00     2
 25 JUN   59 Floral Ini     302   0.51  10.9   11  3.6  0.00  0.00  0.00  0.00     3
  1 OCT  157 75% Silkin    3064   0.63  22.9   28  0.9  0.00  0.30  0.00  0.00     4
 27 OCT  183 Beg Gr Fil    3894   0.26  22.9   29  0.8  0.00  0.68  0.00  0.00     5
  1 FEB  280 End Gr Fil    5154   0.03  22.9   44  0.9  0.00  0.43  0.00  0.00     6
  7 FEB  286 Maturity      5154   0.03  22.9   44  0.9  0.00  0.03  0.00  0.00    10
  7 FEB  286 Harvest       5154   0.03  22.9   44  0.9  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     157          -99
      Physiological maturity day (dap)                       286          -99
      Yield at harvest maturity (kg [dm]/ha)                1521          -99
      Number at maturity (no/m2)                             278          -99
      Unit wt at maturity (g [dm]/unit)                   0.5472          -99
      Number at maturity (no/unit)                          69.5          -99
      Tops weight at maturity (kg [dm]/ha)                  5154          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    3669          -99
      Leaf area index, maximum                              1.38          -99
      Harvest index at maturity                            0.295          -99
      Grain N at maturity (kg/ha)                             17          -99
      Tops N at maturity (kg/ha)                              44          -99
      Stem N at maturity (kg/ha)                              27          -99
      Grain N at maturity (%)                                1.1          -99
      Tops weight at anthesis (kg [dm]/ha)                  2994          -99
      Tops N at anthesis (kg/ha)                              28          -99
      Leaf number per stem at maturity                     22.87          -99
      Emergence day (dap)                                     11          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    43  18.5   9.9  14.2  20.1  12.05  380.0  588.1  119.5  189.9    0    0    0    0    0   38  0.000  0.000  0.002  0.006  0.000  0.000
 End Juvenil-Floral Init    5  17.8   8.9  13.4  20.8  12.06  380.0    3.8   15.9   21.7    0    0    0    0    0    3  0.000  0.000  0.000  0.001  0.000  0.000
 Floral Init-End Lf Grow   98  17.6   9.6  13.6  18.9  12.03  380.0   3837    370    375    0    0    0    0    0   98  0.000  0.000  0.149  0.290  0.000  0.000
 End Lf Grth-Beg Grn Fil   26  19.0  10.2  14.6  21.5  11.98  380.0   1105    107    121    0    0    0    0    0   26  0.000  0.000  0.411  0.676  0.000  0.000
 Grain Filling Phase       97  20.1  10.5  15.3  22.5  11.95  380.0  440.9  230.4  493.1    0    0    0    0    0   66  0.000  0.000  0.201  0.435  0.000  0.000
   
 Planting to Harvest      286  18.9  10.0  14.5  20.8  12.00  380.0   6488    888   1287    0    0    0    0    0  246  0.000  0.000  0.157  0.310  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 286 days 

 Precipitation during growing season      6487.5 mm[rain]
   Dry Matter Productivity                  0.08 kg[DM]/m3[rain]          =    0.8 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.02 kg[grain yield]/m3[rain] =    0.2 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  888.5 mm[ET]
   Dry Matter Productivity                  0.58 kg[DM]/m3[ET]            =    5.8 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.17 kg[grain yield]/m3[ET]   =    1.7 kg[yield]/ha per mm[ET]

 Transpiration during growing season       261.5 mm[EP]
   Dry Matter Productivity                  1.97 kg[DM]/m3[EP]            =   19.7 kg[DM]/ha per mm[EP]
   Yield Productivity                       0.58 kg[grain yield]/m3[EP]   =    5.8 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                  34.4 kg[DM]/kg[N applied]
   Yield Productivity                       10.1 kg[yield]/kg[N applied]

 N uptake during growing season              103 kg[N uptake]/ha
   Dry Matter Productivity                  50.0 kg[DM]/kg[N uptake]
   Yield Productivity                       14.8 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     1521 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:40

*RUN  37        : SAMPLE 37                 MZCER048 EXPEFILE   37
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 37   : SAMPLE 37                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEBK   2021
 SOIL           : IB00000019     TEXTURE : yLoam -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:253.9mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.223 0.352 0.437 0.129 0.352   1.00   1.21   6.16   0.00   0.00   1.80
  5- 15 0.236 0.365 0.444 0.129 0.365   0.85   1.23   6.24   0.00   0.00   1.52
 15- 30 0.235 0.363 0.442 0.128 0.363   0.70   1.24   6.26   0.00   0.00   1.07
 30- 45 0.250 0.377 0.450 0.127 0.377   0.50   1.29   6.37   0.00   0.00   0.68
 45- 60 0.250 0.377 0.450 0.127 0.377   0.50   1.29   6.37   0.00   0.00   0.68
 60- 80 0.249 0.377 0.449 0.128 0.377   0.38   1.35   6.51   0.00   0.00   0.40
 80-100 0.249 0.377 0.449 0.128 0.377   0.38   1.35   6.51   0.00   0.00   0.40
100-150 0.240 0.366 0.442 0.126 0.366   0.05   1.40   6.69   0.00   0.00   0.22
150-200 0.240 0.366 0.442 0.126 0.366   0.05   1.40   6.69   0.00   0.00   0.22

TOT-200  48.5  73.8  88.9  25.4  73.8  <--cm   -  kg/ha-->    0.0    0.0 128204
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    37     SAMPLE 37               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  6 MAY   10 Emergence       16   0.00   1.8    1  4.4  0.00  0.00  0.00  0.00     1
 16 JUN   51 End Juveni     229   0.41  10.2    9  3.9  0.00  0.01  0.00  0.00     2
 21 JUN   56 Floral Ini     329   0.55  11.1   12  3.6  0.00  0.00  0.00  0.00     3
 23 SEP  150 75% Silkin    5943   2.51  23.2   87  1.5  0.00  0.07  0.00  0.00     4
 18 OCT  175 Beg Gr Fil   11566   1.57  23.2   89  0.8  0.03  0.37  0.00  0.00     5
 25 JAN  274 End Gr Fil   14187   0.05  23.2   79  0.6  0.66  0.58  0.00  0.00     6
 31 JAN  280 Maturity     14187   0.05  23.2   79  0.6  0.00  0.75  0.00  0.00    10
 31 JAN  280 Harvest      14187   0.05  23.2   79  0.6  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     150          -99
      Physiological maturity day (dap)                       280          -99
      Yield at harvest maturity (kg [dm]/ha)                4728          -99
      Number at maturity (no/m2)                            1215          -99
      Unit wt at maturity (g [dm]/unit)                   0.3891          -99
      Number at maturity (no/unit)                         303.8          -99
      Tops weight at maturity (kg [dm]/ha)                 14187          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    9513          -99
      Leaf area index, maximum                              2.81          -99
      Harvest index at maturity                            0.333          -99
      Grain N at maturity (kg/ha)                             48          -99
      Tops N at maturity (kg/ha)                              79          -99
      Stem N at maturity (kg/ha)                              31          -99
      Grain N at maturity (%)                                1.0          -99
      Tops weight at anthesis (kg [dm]/ha)                  5625          -99
      Tops N at anthesis (kg/ha)                              87          -99
      Leaf number per stem at maturity                     23.23          -99
      Emergence day (dap)                                     10          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    41  18.9  10.3  14.6  20.6  12.01  380.0  151.5   71.7  185.9    0    0    0    0    0   14  0.000  0.000  0.003  0.007  0.000  0.000
 End Juvenil-Floral Init    5  18.7  10.2  14.4  17.9  12.02  380.0    0.4    6.1   18.9    0    0    0    0    0    2  0.000  0.000  0.000  0.001  0.000  0.000
 Floral Init-End Lf Grow   94  18.0   9.8  13.9  18.4  12.01  380.0  519.3  303.5  339.3    0    0    0    0    0   75  0.000  0.000  0.025  0.063  0.000  0.000
 End Lf Grth-Beg Grn Fil   25  19.5  10.1  14.8  22.2  12.00  380.0   25.2  108.9  109.8    0    0    0    0    0   24  0.000  0.016  0.145  0.363  0.000  0.000
 Grain Filling Phase       99  20.3  10.2  15.2  22.5  11.99  380.0   47.3   83.0  482.0    0    0    0    0    0   45  0.534  0.659  0.292  0.573  0.000  0.000
   
 Planting to Harvest      280  19.2  10.1  14.7  20.8  12.00  380.0  779.9  599.0 1220.4    0    0    0    0    0  167  0.189  0.234  0.136  0.273  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 280 days 

 Precipitation during growing season       779.9 mm[rain]
   Dry Matter Productivity                  1.82 kg[DM]/m3[rain]          =   18.2 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.61 kg[grain yield]/m3[rain] =    6.1 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  599.0 mm[ET]
   Dry Matter Productivity                  2.37 kg[DM]/m3[ET]            =   23.7 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.79 kg[grain yield]/m3[ET]   =    7.9 kg[yield]/ha per mm[ET]

 Transpiration during growing season       377.9 mm[EP]
   Dry Matter Productivity                  3.75 kg[DM]/m3[EP]            =   37.5 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.25 kg[grain yield]/m3[EP]   =   12.5 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                  94.6 kg[DM]/kg[N applied]
   Yield Productivity                       31.5 kg[yield]/kg[N applied]

 N uptake during growing season              172 kg[N uptake]/ha
   Dry Matter Productivity                  82.5 kg[DM]/kg[N uptake]
   Yield Productivity                       27.5 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     4728 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:40

*RUN  38        : SAMPLE 38                 MZCER048 EXPEFILE   38
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 38   : SAMPLE 38                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEBL   2021
 SOIL           : IB00000015     TEXTURE : Loam  -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:248.0mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.208 0.331 0.424 0.123 0.331   1.00   1.18   5.85   0.00   0.00   2.55
  5- 15 0.224 0.349 0.433 0.125 0.349   0.85   1.21   5.93   0.00   0.00   2.16
 15- 30 0.226 0.350 0.433 0.124 0.350   0.70   1.22   5.88   0.00   0.00   1.58
 30- 45 0.246 0.372 0.445 0.126 0.372   0.50   1.28   5.99   0.00   0.00   1.01
 45- 60 0.246 0.372 0.445 0.126 0.372   0.50   1.28   5.99   0.00   0.00   1.01
 60- 80 0.245 0.370 0.444 0.125 0.370   0.38   1.35   6.13   0.00   0.00   0.59
 80-100 0.245 0.370 0.444 0.125 0.370   0.38   1.35   6.13   0.00   0.00   0.59
100-150 0.232 0.355 0.435 0.123 0.355   0.05   1.42   6.29   0.00   0.00   0.34
150-200 0.232 0.355 0.435 0.123 0.355   0.05   1.42   6.29   0.00   0.00   0.34

TOT-200  47.0  71.9  87.6  24.8  71.9  <--cm   -  kg/ha-->    0.0    0.0 189019
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    38     SAMPLE 38               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  3 MAY    7 Emergence       16   0.00   2.0    1  4.4  0.00  0.01  0.00  0.00     1
 28 MAY   32 End Juveni     244   0.43  10.3    9  3.7  0.00  0.00  0.00  0.00     2
  2 JUN   37 Floral Ini     447   0.70  11.9   16  3.6  0.00  0.00  0.00  0.00     3
 26 JUL   91 75% Silkin    5053   1.93  24.9   74  1.5  0.10  0.20  0.00  0.00     4
 12 AUG  108 Beg Gr Fil    7958   1.48  24.9   78  1.0  0.00  0.31  0.00  0.00     5
 14 OCT  171 End Gr Fil   12274   0.53  24.9   95  0.8  0.00  0.33  0.00  0.00     6
 18 OCT  175 Maturity     12274   0.53  24.9   95  0.8  0.00  0.45  0.00  0.00    10
 18 OCT  175 Harvest      12274   0.53  24.9   95  0.8  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                      91          -99
      Physiological maturity day (dap)                       175          -99
      Yield at harvest maturity (kg [dm]/ha)                4392          -99
      Number at maturity (no/m2)                            1160          -99
      Unit wt at maturity (g [dm]/unit)                   0.3785          -99
      Number at maturity (no/unit)                         290.1          -99
      Tops weight at maturity (kg [dm]/ha)                 12274          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    7934          -99
      Leaf area index, maximum                              2.11          -99
      Harvest index at maturity                            0.358          -99
      Grain N at maturity (kg/ha)                             56          -99
      Tops N at maturity (kg/ha)                              95          -99
      Stem N at maturity (kg/ha)                              39          -99
      Grain N at maturity (%)                                1.3          -99
      Tops weight at anthesis (kg [dm]/ha)                  4981          -99
      Tops N at anthesis (kg/ha)                              74          -99
      Leaf number per stem at maturity                     24.89          -99
      Emergence day (dap)                                      7          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    25  23.4  15.1  19.2  20.5  12.05  380.0  216.9   63.2  125.2    0    0    0    0    0   15  0.000  0.000  0.002  0.005  0.000  0.000
 End Juvenil-Floral Init    5  25.3  14.6  20.0  23.0  12.06  380.0    1.4   11.3   27.5    0    0    0    0    0    2  0.000  0.000  0.001  0.001  0.000  0.000
 Floral Init-End Lf Grow   54  23.5  14.4  18.9  18.7  12.06  380.0  470.4  163.9  222.7    0    0    0    0    0   31  0.059  0.101  0.078  0.194  0.000  0.000
 End Lf Grth-Beg Grn Fil   17  21.9  14.3  18.1  18.3  12.04  380.0  144.0   66.2   66.8    0    0    0    0    0   13  0.000  0.000  0.123  0.307  0.000  0.000
 Grain Filling Phase       63  24.0  14.4  19.2  22.0  12.01  380.0  350.0  277.6  316.7    0    0    0    0    0   53  0.000  0.000  0.131  0.327  0.000  0.000
   
 Planting to Harvest      175  23.7  14.5  19.1  20.5  12.03  380.0   1184    600    823    0    0    0    0    0  117  0.018  0.031  0.087  0.219  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 175 days 

 Precipitation during growing season      1183.7 mm[rain]
   Dry Matter Productivity                  1.04 kg[DM]/m3[rain]          =   10.4 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.37 kg[grain yield]/m3[rain] =    3.7 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  600.5 mm[ET]
   Dry Matter Productivity                  2.04 kg[DM]/m3[ET]            =   20.4 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.73 kg[grain yield]/m3[ET]   =    7.3 kg[yield]/ha per mm[ET]

 Transpiration during growing season       359.2 mm[EP]
   Dry Matter Productivity                  3.42 kg[DM]/m3[EP]            =   34.2 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.22 kg[grain yield]/m3[EP]   =   12.2 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                  81.8 kg[DM]/kg[N applied]
   Yield Productivity                       29.3 kg[yield]/kg[N applied]

 N uptake during growing season              152 kg[N uptake]/ha
   Dry Matter Productivity                  80.8 kg[DM]/kg[N uptake]
   Yield Productivity                       28.9 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     4392 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:40

*RUN  39        : SAMPLE 39                 MZCER048 EXPEFILE   39
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 39   : SAMPLE 39                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEBM   2021
 SOIL           : IB00000013     TEXTURE : yLoam -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:248.6mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.220 0.346 0.433 0.126 0.346   1.00   1.21   6.15   0.00   0.00   1.90
  5- 15 0.232 0.359 0.439 0.127 0.359   0.85   1.23   6.21   0.00   0.00   1.61
 15- 30 0.232 0.357 0.438 0.125 0.357   0.70   1.24   6.20   0.00   0.00   1.15
 30- 45 0.245 0.371 0.445 0.126 0.371   0.50   1.29   6.33   0.00   0.00   0.73
 45- 60 0.245 0.371 0.445 0.126 0.371   0.50   1.29   6.33   0.00   0.00   0.73
 60- 80 0.245 0.370 0.444 0.125 0.370   0.38   1.36   6.48   0.00   0.00   0.43
 80-100 0.245 0.370 0.444 0.125 0.370   0.38   1.36   6.48   0.00   0.00   0.43
100-150 0.236 0.359 0.437 0.123 0.359   0.05   1.41   6.67   0.00   0.00   0.25
150-200 0.236 0.359 0.437 0.123 0.359   0.05   1.41   6.67   0.00   0.00   0.25

TOT-200  47.7  72.5  87.9  24.9  72.5  <--cm   -  kg/ha-->    0.0    0.0 139581
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    39     SAMPLE 39               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 27 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 28 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  7 MAY   10 Emergence       16   0.00   1.8    1  4.4  0.00  0.00  0.00  0.00     1
 15 JUN   49 End Juveni     222   0.40  10.1    9  3.9  0.00  0.01  0.00  0.00     2
 20 JUN   54 Floral Ini     314   0.53  11.0   11  3.6  0.00  0.00  0.00  0.00     3
 18 SEP  144 75% Silkin    3488   0.88  23.0   36  1.0  0.00  0.26  0.00  0.00     4
 11 OCT  167 Beg Gr Fil    4800   0.43  23.0   36  0.7  0.00  0.62  0.00  0.00     5
 11 JAN  259 End Gr Fil    6329   0.03  23.0   45  0.7  0.00  0.55  0.00  0.00     6
 16 JAN  264 Maturity      6329   0.03  23.0   45  0.7  0.00  0.34  0.00  0.00    10
 16 JAN  264 Harvest       6329   0.03  23.0   45  0.7  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     144          -99
      Physiological maturity day (dap)                       264          -99
      Yield at harvest maturity (kg [dm]/ha)                1955          -99
      Number at maturity (no/m2)                             362          -99
      Unit wt at maturity (g [dm]/unit)                   0.5405          -99
      Number at maturity (no/unit)                          90.4          -99
      Tops weight at maturity (kg [dm]/ha)                  6329          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    4414          -99
      Leaf area index, maximum                              1.63          -99
      Harvest index at maturity                            0.309          -99
      Grain N at maturity (kg/ha)                             19          -99
      Tops N at maturity (kg/ha)                              45          -99
      Stem N at maturity (kg/ha)                              26          -99
      Grain N at maturity (%)                                1.0          -99
      Tops weight at anthesis (kg [dm]/ha)                  3452          -99
      Tops N at anthesis (kg/ha)                              36          -99
      Leaf number per stem at maturity                     23.01          -99
      Emergence day (dap)                                     10          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    39  19.6  10.1  14.9  19.4  12.06  380.0  518.4  106.3  169.4    0    0    0    0    0   34  0.000  0.000  0.002  0.006  0.000  0.000
 End Juvenil-Floral Init    5  18.4  10.4  14.4  17.0  12.06  380.0   87.6   18.2   18.2    0    0    0    0    0    5  0.000  0.000  0.000  0.000  0.000  0.000
 Floral Init-End Lf Grow   90  18.4   9.9  14.2  17.7  12.04  380.0   3431    317    322    0    0    0    0    0   88  0.000  0.000  0.108  0.250  0.000  0.000
 End Lf Grth-Beg Grn Fil   23  19.9  10.4  15.2  19.4  11.99  380.0   1211     96     97    0    0    0    0    0   23  0.000  0.000  0.337  0.614  0.000  0.000
 Grain Filling Phase       92  21.4  10.1  15.8  21.3  11.95  380.0  506.7  255.5  446.0    0    0    0    0    0   70  0.000  0.000  0.276  0.551  0.000  0.000
   
 Planting to Harvest      264  20.0  10.1  15.0  19.6  12.01  380.0   6268    830   1129    0    0    0    0    0  231  0.000  0.000  0.165  0.338  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 264 days 

 Precipitation during growing season      6267.6 mm[rain]
   Dry Matter Productivity                  0.10 kg[DM]/m3[rain]          =    1.0 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.03 kg[grain yield]/m3[rain] =    0.3 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  829.6 mm[ET]
   Dry Matter Productivity                  0.76 kg[DM]/m3[ET]            =    7.6 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.24 kg[grain yield]/m3[ET]   =    2.4 kg[yield]/ha per mm[ET]

 Transpiration during growing season       270.7 mm[EP]
   Dry Matter Productivity                  2.34 kg[DM]/m3[EP]            =   23.4 kg[DM]/ha per mm[EP]
   Yield Productivity                       0.72 kg[grain yield]/m3[EP]   =    7.2 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                  42.2 kg[DM]/kg[N applied]
   Yield Productivity                       13.0 kg[yield]/kg[N applied]

 N uptake during growing season              100 kg[N uptake]/ha
   Dry Matter Productivity                  63.3 kg[DM]/kg[N uptake]
   Yield Productivity                       19.5 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     1955 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:40

*RUN  40        : SAMPLE 40                 MZCER048 EXPEFILE   40
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 40   : SAMPLE 40                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEBN   2021
 SOIL           : IB00000017     TEXTURE : yLoam -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:255.3mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.225 0.353 0.437 0.128 0.353   1.00   1.14   5.78   0.00   0.00   2.49
  5- 15 0.236 0.364 0.443 0.128 0.364   0.85   1.16   5.85   0.00   0.00   2.11
 15- 30 0.243 0.371 0.448 0.128 0.371   0.70   1.18   5.82   0.00   0.00   1.53
 30- 45 0.256 0.385 0.456 0.129 0.385   0.50   1.23   5.93   0.00   0.00   0.98
 45- 60 0.256 0.385 0.456 0.129 0.385   0.50   1.23   5.93   0.00   0.00   0.98
 60- 80 0.256 0.384 0.455 0.128 0.384   0.38   1.31   6.07   0.00   0.00   0.57
 80-100 0.256 0.384 0.455 0.128 0.384   0.38   1.31   6.07   0.00   0.00   0.57
100-150 0.247 0.374 0.448 0.127 0.374   0.05   1.36   6.25   0.00   0.00   0.32
150-200 0.247 0.374 0.448 0.127 0.374   0.05   1.36   6.25   0.00   0.00   0.32

TOT-200  49.8  75.3  90.0  25.5  75.3  <--cm   -  kg/ha-->    0.0    0.0 175300
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    40     SAMPLE 40               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  4 MAY    8 Emergence       16   0.00   1.9    1  4.4  0.00  0.00  0.00  0.00     1
  3 JUN   38 End Juveni     226   0.41  10.1    8  3.7  0.00  0.00  0.00  0.00     2
  8 JUN   43 Floral Ini     380   0.62  11.5   14  3.6  0.00  0.00  0.00  0.00     3
 15 AUG  111 75% Silkin    6089   3.25  23.8  104  1.7  0.00  0.03  0.00  0.00     4
  3 SEP  130 Beg Gr Fil   11947   2.97  23.8  109  0.9  0.00  0.10  0.00  0.00     5
 18 NOV  206 End Gr Fil   20164   0.45  23.8  126  0.6  0.00  0.50  0.00  0.00     6
 23 NOV  211 Maturity     20164   0.45  23.8  126  0.6  0.00  0.85  0.00  0.00    10
 23 NOV  211 Harvest      20164   0.45  23.8  126  0.6  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     111          -99
      Physiological maturity day (dap)                       211          -99
      Yield at harvest maturity (kg [dm]/ha)                9098          -99
      Number at maturity (no/m2)                            2000          -99
      Unit wt at maturity (g [dm]/unit)                   0.4549          -99
      Number at maturity (no/unit)                         500.0          -99
      Tops weight at maturity (kg [dm]/ha)                 20164          -99
      By-product produced (stalk) at maturity (kg[dm]/ha   11122          -99
      Leaf area index, maximum                              3.26          -99
      Harvest index at maturity                            0.451          -99
      Grain N at maturity (kg/ha)                             97          -99
      Tops N at maturity (kg/ha)                             126          -99
      Stem N at maturity (kg/ha)                              29          -99
      Grain N at maturity (%)                                1.1          -99
      Tops weight at anthesis (kg [dm]/ha)                  5815          -99
      Tops N at anthesis (kg/ha)                             104          -99
      Leaf number per stem at maturity                     23.82          -99
      Emergence day (dap)                                      8          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    30  20.9  13.4  17.2  21.0  12.06  380.0  182.4   68.3  147.1    0    0    0    0    0   16  0.000  0.000  0.002  0.006  0.000  0.000
 End Juvenil-Floral Init    5  22.4  13.2  17.8  21.8  12.07  380.0    0.1    8.3   24.8    0    0    0    0    0    1  0.000  0.000  0.000  0.000  0.000  0.000
 Floral Init-End Lf Grow   68  20.2  12.7  16.4  18.4  12.06  380.0  644.0  210.2  258.6    0    0    0    0    0   46  0.000  0.002  0.010  0.025  0.000  0.000
 End Lf Grth-Beg Grn Fil   19  21.1  12.5  16.8  21.9  12.03  380.0   60.3   83.9   84.4    0    0    0    0    0   14  0.000  0.000  0.038  0.095  0.000  0.000
 Grain Filling Phase       76  21.8  12.9  17.3  22.6  11.98  380.0  260.5  284.5  366.3    0    0    0    0    0   40  0.000  0.000  0.277  0.491  0.000  0.000
   
 Planting to Harvest      211  21.2  12.9  17.0  21.0  12.02  380.0   1184    668    953    0    0    0    0    0  122  0.000  0.001  0.122  0.215  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 211 days 

 Precipitation during growing season      1184.4 mm[rain]
   Dry Matter Productivity                  1.70 kg[DM]/m3[rain]          =   17.0 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.77 kg[grain yield]/m3[rain] =    7.7 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  668.0 mm[ET]
   Dry Matter Productivity                  3.02 kg[DM]/m3[ET]            =   30.2 kg[DM]/ha per mm[ET]
   Yield Productivity                       1.36 kg[grain yield]/m3[ET]   =   13.6 kg[yield]/ha per mm[ET]

 Transpiration during growing season       511.5 mm[EP]
   Dry Matter Productivity                  3.94 kg[DM]/m3[EP]            =   39.4 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.78 kg[grain yield]/m3[EP]   =   17.8 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                 134.4 kg[DM]/kg[N applied]
   Yield Productivity                       60.7 kg[yield]/kg[N applied]

 N uptake during growing season              219 kg[N uptake]/ha
   Dry Matter Productivity                  92.1 kg[DM]/kg[N uptake]
   Yield Productivity                       41.5 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     9098 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:40

*RUN  41        : SAMPLE 41                 MZCER048 EXPEFILE   41
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 41   : SAMPLE 41                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEBO   2021
 SOIL           : IB00000007     TEXTURE : Loam  -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:248.6mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.204 0.327 0.423 0.123 0.327   1.00   1.17   5.74   0.00   0.00   2.89
  5- 15 0.221 0.346 0.432 0.125 0.346   0.85   1.19   5.81   0.00   0.00   2.45
 15- 30 0.223 0.348 0.433 0.125 0.348   0.70   1.20   5.78   0.00   0.00   1.82
 30- 45 0.242 0.368 0.444 0.126 0.368   0.50   1.26   5.89   0.00   0.00   1.17
 45- 60 0.242 0.368 0.444 0.126 0.368   0.50   1.26   5.89   0.00   0.00   1.17
 60- 80 0.241 0.367 0.443 0.126 0.367   0.38   1.33   6.03   0.00   0.00   0.68
 80-100 0.241 0.367 0.443 0.126 0.367   0.38   1.33   6.03   0.00   0.00   0.68
100-150 0.228 0.351 0.433 0.123 0.351   0.05   1.38   6.21   0.00   0.00   0.39
150-200 0.228 0.351 0.433 0.123 0.351   0.05   1.38   6.21   0.00   0.00   0.39

TOT-200  46.3  71.1  87.3  24.9  71.1  <--cm   -  kg/ha-->    0.0    0.0 213044
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    41     SAMPLE 41               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  4 MAY    8 Emergence       16   0.00   1.9    1  4.4  0.00  0.00  0.00  0.00     1
  1 JUN   36 End Juveni     223   0.40  10.0    8  3.7  0.00  0.00  0.00  0.00     2
  6 JUN   41 Floral Ini     387   0.62  11.5   14  3.6  0.00  0.00  0.00  0.00     3
  9 AUG  105 75% Silkin    6457   3.35  24.1  115  1.8  0.01  0.03  0.00  0.00     4
 27 AUG  123 Beg Gr Fil   12113   3.20  24.1  119  1.0  0.00  0.03  0.00  0.00     5
  8 NOV  196 End Gr Fil   20622   0.77  24.1  138  0.7  0.00  0.40  0.00  0.00     6
 12 NOV  200 Maturity     20622   0.77  24.1  138  0.7  0.00  0.81  0.00  0.00    10
 12 NOV  200 Harvest      20622   0.77  24.1  138  0.7  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     105          -99
      Physiological maturity day (dap)                       200          -99
      Yield at harvest maturity (kg [dm]/ha)                8747          -99
      Number at maturity (no/m2)                            2000          -99
      Unit wt at maturity (g [dm]/unit)                   0.4374          -99
      Number at maturity (no/unit)                         500.0          -99
      Tops weight at maturity (kg [dm]/ha)                 20622          -99
      By-product produced (stalk) at maturity (kg[dm]/ha   11932          -99
      Leaf area index, maximum                              3.36          -99
      Harvest index at maturity                            0.424          -99
      Grain N at maturity (kg/ha)                            105          -99
      Tops N at maturity (kg/ha)                             138          -99
      Stem N at maturity (kg/ha)                              33          -99
      Grain N at maturity (%)                                1.2          -99
      Tops weight at anthesis (kg [dm]/ha)                  6163          -99
      Tops N at anthesis (kg/ha)                             114          -99
      Leaf number per stem at maturity                     24.12          -99
      Emergence day (dap)                                      8          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    28  21.8  13.6  17.7  20.9  12.05  380.0  182.4   66.0  138.6    0    0    0    0    0   16  0.000  0.000  0.002  0.005  0.000  0.000
 End Juvenil-Floral Init    5  23.4  13.3  18.3  21.9  12.06  380.0    0.1    8.6   25.2    0    0    0    0    0    1  0.000  0.000  0.000  0.001  0.000  0.000
 Floral Init-End Lf Grow   64  21.2  12.9  17.1  18.4  12.06  380.0  582.2  196.7  246.8    0    0    0    0    0   40  0.000  0.005  0.012  0.031  0.000  0.000
 End Lf Grth-Beg Grn Fil   18  21.6  12.7  17.2  21.6  12.03  380.0   65.6   78.8   79.3    0    0    0    0    0   13  0.000  0.000  0.014  0.035  0.000  0.000
 Grain Filling Phase       73  22.6  12.9  17.8  22.5  11.99  380.0  316.7  304.9  348.3    0    0    0    0    0   45  0.000  0.000  0.202  0.385  0.000  0.000
   
 Planting to Harvest      200  22.1  13.1  17.6  20.9  12.03  380.0   1184    669    904    0    0    0    0    0  120  0.000  0.002  0.091  0.170  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 200 days 

 Precipitation during growing season      1184.1 mm[rain]
   Dry Matter Productivity                  1.74 kg[DM]/m3[rain]          =   17.4 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.74 kg[grain yield]/m3[rain] =    7.4 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  669.0 mm[ET]
   Dry Matter Productivity                  3.08 kg[DM]/m3[ET]            =   30.8 kg[DM]/ha per mm[ET]
   Yield Productivity                       1.31 kg[grain yield]/m3[ET]   =   13.1 kg[yield]/ha per mm[ET]

 Transpiration during growing season       524.9 mm[EP]
   Dry Matter Productivity                  3.93 kg[DM]/m3[EP]            =   39.3 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.67 kg[grain yield]/m3[EP]   =   16.7 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                 137.5 kg[DM]/kg[N applied]
   Yield Productivity                       58.3 kg[yield]/kg[N applied]

 N uptake during growing season              229 kg[N uptake]/ha
   Dry Matter Productivity                  90.1 kg[DM]/kg[N uptake]
   Yield Productivity                       38.2 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     8747 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:40

*RUN  42        : SAMPLE 42                 MZCER048 EXPEFILE   42
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 42   : SAMPLE 42                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEBP   2021
 SOIL           : IB00000011     TEXTURE : yLoam -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:247.6mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.217 0.342 0.430 0.125 0.342   1.00   1.20   5.99   0.00   0.00   2.16
  5- 15 0.229 0.354 0.436 0.125 0.354   0.85   1.22   6.06   0.00   0.00   1.83
 15- 30 0.231 0.356 0.437 0.125 0.356   0.70   1.23   6.02   0.00   0.00   1.32
 30- 45 0.243 0.368 0.443 0.125 0.368   0.50   1.28   6.15   0.00   0.00   0.85
 45- 60 0.243 0.368 0.443 0.125 0.368   0.50   1.28   6.15   0.00   0.00   0.85
 60- 80 0.244 0.368 0.443 0.124 0.368   0.38   1.35   6.31   0.00   0.00   0.49
 80-100 0.244 0.368 0.443 0.124 0.368   0.38   1.35   6.31   0.00   0.00   0.49
100-150 0.235 0.358 0.436 0.123 0.358   0.05   1.41   6.51   0.00   0.00   0.28
150-200 0.235 0.358 0.436 0.123 0.358   0.05   1.41   6.51   0.00   0.00   0.28

TOT-200  47.4  72.2  87.7  24.8  72.2  <--cm   -  kg/ha-->    0.0    0.0 158220
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    42     SAMPLE 42               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  3 MAY    7 Emergence       16   0.00   2.0    1  4.4  0.00  0.01  0.00  0.00     1
 31 MAY   35 End Juveni     262   0.46  10.5   10  3.7  0.00  0.00  0.00  0.00     2
  5 JUN   40 Floral Ini     434   0.69  11.9   16  3.6  0.00  0.00  0.00  0.00     3
  5 AUG  101 75% Silkin    5923   2.48  24.6   91  1.5  0.03  0.13  0.00  0.00     4
 23 AUG  119 Beg Gr Fil    9977   1.99  24.6   96  1.0  0.00  0.24  0.00  0.00     5
 30 OCT  187 End Gr Fil   15622   0.41  24.6  102  0.7  0.20  0.47  0.00  0.00     6
  3 NOV  191 Maturity     15622   0.41  24.6  102  0.7  0.78  0.78  0.00  0.00    10
  3 NOV  191 Harvest      15622   0.41  24.6  102  0.7  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     101          -99
      Physiological maturity day (dap)                       191          -99
      Yield at harvest maturity (kg [dm]/ha)                5894          -99
      Number at maturity (no/m2)                            1560          -99
      Unit wt at maturity (g [dm]/unit)                   0.3779          -99
      Number at maturity (no/unit)                         389.9          -99
      Tops weight at maturity (kg [dm]/ha)                 15622          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    9782          -99
      Leaf area index, maximum                              2.54          -99
      Harvest index at maturity                            0.377          -99
      Grain N at maturity (kg/ha)                             67          -99
      Tops N at maturity (kg/ha)                             102          -99
      Stem N at maturity (kg/ha)                              34          -99
      Grain N at maturity (%)                                1.1          -99
      Tops weight at anthesis (kg [dm]/ha)                  5631          -99
      Tops N at anthesis (kg/ha)                              90          -99
      Leaf number per stem at maturity                     24.62          -99
      Emergence day (dap)                                      7          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    28  23.0  13.5  18.2  20.1  12.02  380.0  173.3   74.7  134.8    0    0    0    0    0   16  0.000  0.000  0.002  0.006  0.000  0.000
 End Juvenil-Floral Init    5  24.0  12.2  18.1  22.3  12.02  380.0    0.0    9.2   25.5    0    0    0    0    0    0  0.000  0.000  0.001  0.002  0.000  0.000
 Floral Init-End Lf Grow   61  22.3  12.7  17.5  18.0  12.02  380.0  394.9  182.4  234.2    0    0    0    0    0   39  0.009  0.031  0.052  0.131  0.000  0.000
 End Lf Grth-Beg Grn Fil   18  22.2  12.6  17.4  20.4  12.02  380.0   48.5   76.0   76.7    0    0    0    0    0   14  0.000  0.000  0.096  0.240  0.000  0.000
 Grain Filling Phase       68  24.2  12.9  18.5  22.2  12.00  380.0  104.4  235.0  337.8    0    0    0    0    0   58  0.135  0.187  0.230  0.462  0.000  0.000
   
 Planting to Harvest      191  23.2  12.9  18.1  20.4  12.01  380.0  736.2  594.8  869.5    0    0    0    0    0  134  0.065  0.093  0.119  0.246  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 191 days 

 Precipitation during growing season       736.2 mm[rain]
   Dry Matter Productivity                  2.12 kg[DM]/m3[rain]          =   21.2 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.80 kg[grain yield]/m3[rain] =    8.0 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  594.8 mm[ET]
   Dry Matter Productivity                  2.63 kg[DM]/m3[ET]            =   26.3 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.99 kg[grain yield]/m3[ET]   =    9.9 kg[yield]/ha per mm[ET]

 Transpiration during growing season       395.4 mm[EP]
   Dry Matter Productivity                  3.95 kg[DM]/m3[EP]            =   39.5 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.49 kg[grain yield]/m3[EP]   =   14.9 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                 104.1 kg[DM]/kg[N applied]
   Yield Productivity                       39.3 kg[yield]/kg[N applied]

 N uptake during growing season              164 kg[N uptake]/ha
   Dry Matter Productivity                  95.3 kg[DM]/kg[N uptake]
   Yield Productivity                       35.9 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     5894 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:40

*RUN  43        : SAMPLE 43                 MZCER048 EXPEFILE   43
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 43   : SAMPLE 43                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEBQ   2021
 SOIL           : IB00000020     TEXTURE : ClayL -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:248.2mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.217 0.341 0.430 0.124 0.341   1.00   1.16   5.66   0.00   0.00   3.03
  5- 15 0.231 0.356 0.437 0.125 0.356   0.85   1.18   5.72   0.00   0.00   2.56
 15- 30 0.235 0.360 0.439 0.125 0.360   0.70   1.19   5.68   0.00   0.00   1.93
 30- 45 0.251 0.377 0.448 0.126 0.377   0.50   1.24   5.79   0.00   0.00   1.24
 45- 60 0.251 0.377 0.448 0.126 0.377   0.50   1.24   5.79   0.00   0.00   1.24
 60- 80 0.251 0.376 0.447 0.125 0.376   0.38   1.31   5.92   0.00   0.00   0.72
 80-100 0.251 0.376 0.447 0.125 0.376   0.38   1.31   5.92   0.00   0.00   0.72
100-150 0.239 0.362 0.439 0.123 0.362   0.05   1.36   6.08   0.00   0.00   0.41
150-200 0.239 0.362 0.439 0.123 0.362   0.05   1.36   6.08   0.00   0.00   0.41

TOT-200  48.4  73.2  88.3  24.8  73.2  <--cm   -  kg/ha-->    0.0    0.0 221849
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    43     SAMPLE 43               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  4 MAY    8 Emergence       16   0.00   1.9    1  4.4  0.00  0.00  0.00  0.00     1
  1 JUN   36 End Juveni     222   0.40  10.0    8  3.7  0.00  0.00  0.00  0.00     2
  6 JUN   41 Floral Ini     387   0.62  11.5   14  3.6  0.00  0.00  0.00  0.00     3
  9 AUG  105 75% Silkin    6114   3.24  24.1  109  1.8  0.01  0.03  0.00  0.00     4
 27 AUG  123 Beg Gr Fil   11782   3.09  24.1  114  1.0  0.00  0.02  0.00  0.00     5
  8 NOV  196 End Gr Fil   20170   0.70  24.1  133  0.7  0.00  0.41  0.00  0.00     6
 12 NOV  200 Maturity     20170   0.70  24.1  133  0.7  0.00  0.84  0.00  0.00    10
 12 NOV  200 Harvest      20170   0.70  24.1  133  0.7  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     105          -99
      Physiological maturity day (dap)                       200          -99
      Yield at harvest maturity (kg [dm]/ha)                8747          -99
      Number at maturity (no/m2)                            2000          -99
      Unit wt at maturity (g [dm]/unit)                   0.4374          -99
      Number at maturity (no/unit)                         500.0          -99
      Tops weight at maturity (kg [dm]/ha)                 20170          -99
      By-product produced (stalk) at maturity (kg[dm]/ha   11478          -99
      Leaf area index, maximum                              3.24          -99
      Harvest index at maturity                            0.434          -99
      Grain N at maturity (kg/ha)                            103          -99
      Tops N at maturity (kg/ha)                             133          -99
      Stem N at maturity (kg/ha)                              30          -99
      Grain N at maturity (%)                                1.2          -99
      Tops weight at anthesis (kg [dm]/ha)                  5821          -99
      Tops N at anthesis (kg/ha)                             109          -99
      Leaf number per stem at maturity                     24.12          -99
      Emergence day (dap)                                      8          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    28  21.8  13.6  17.7  20.9  12.05  380.0  182.4   66.0  138.6    0    0    0    0    0   16  0.000  0.000  0.002  0.005  0.000  0.000
 End Juvenil-Floral Init    5  23.4  13.3  18.3  21.9  12.06  380.0    0.1    8.7   25.2    0    0    0    0    0    1  0.000  0.000  0.000  0.001  0.000  0.000
 Floral Init-End Lf Grow   64  21.2  12.9  17.1  18.4  12.06  380.0  582.2  196.6  247.0    0    0    0    0    0   40  0.000  0.011  0.013  0.033  0.000  0.000
 End Lf Grth-Beg Grn Fil   18  21.6  12.7  17.2  21.6  12.03  380.0   65.6   78.9   79.4    0    0    0    0    0   13  0.000  0.000  0.007  0.017  0.000  0.000
 Grain Filling Phase       73  22.6  12.9  17.8  22.5  11.99  380.0  316.7  302.7  349.1    0    0    0    0    0   45  0.000  0.000  0.219  0.400  0.000  0.000
   
 Planting to Harvest      200  22.1  13.1  17.6  20.9  12.03  380.0   1184    666    906    0    0    0    0    0  120  0.000  0.003  0.097  0.175  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 200 days 

 Precipitation during growing season      1184.1 mm[rain]
   Dry Matter Productivity                  1.70 kg[DM]/m3[rain]          =   17.0 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.74 kg[grain yield]/m3[rain] =    7.4 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  666.5 mm[ET]
   Dry Matter Productivity                  3.03 kg[DM]/m3[ET]            =   30.3 kg[DM]/ha per mm[ET]
   Yield Productivity                       1.31 kg[grain yield]/m3[ET]   =   13.1 kg[yield]/ha per mm[ET]

 Transpiration during growing season       518.5 mm[EP]
   Dry Matter Productivity                  3.89 kg[DM]/m3[EP]            =   38.9 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.69 kg[grain yield]/m3[EP]   =   16.9 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                 134.5 kg[DM]/kg[N applied]
   Yield Productivity                       58.3 kg[yield]/kg[N applied]

 N uptake during growing season              226 kg[N uptake]/ha
   Dry Matter Productivity                  89.2 kg[DM]/kg[N uptake]
   Yield Productivity                       38.7 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     8747 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:40

*RUN  44        : SAMPLE 44                 MZCER048 EXPEFILE   44
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 44   : SAMPLE 44                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEBR   2021
 SOIL           : IB00000020     TEXTURE : ClayL -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:248.2mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.217 0.341 0.430 0.124 0.341   1.00   1.16   5.66   0.00   0.00   3.03
  5- 15 0.231 0.356 0.437 0.125 0.356   0.85   1.18   5.72   0.00   0.00   2.56
 15- 30 0.235 0.360 0.439 0.125 0.360   0.70   1.19   5.68   0.00   0.00   1.93
 30- 45 0.251 0.377 0.448 0.126 0.377   0.50   1.24   5.79   0.00   0.00   1.24
 45- 60 0.251 0.377 0.448 0.126 0.377   0.50   1.24   5.79   0.00   0.00   1.24
 60- 80 0.251 0.376 0.447 0.125 0.376   0.38   1.31   5.92   0.00   0.00   0.72
 80-100 0.251 0.376 0.447 0.125 0.376   0.38   1.31   5.92   0.00   0.00   0.72
100-150 0.239 0.362 0.439 0.123 0.362   0.05   1.36   6.08   0.00   0.00   0.41
150-200 0.239 0.362 0.439 0.123 0.362   0.05   1.36   6.08   0.00   0.00   0.41

TOT-200  48.4  73.2  88.3  24.8  73.2  <--cm   -  kg/ha-->    0.0    0.0 221849
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    44     SAMPLE 44               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  6 MAY   10 Emergence       16   0.00   1.8    1  4.4  0.00  0.00  0.00  0.00     1
 16 JUN   51 End Juveni     229   0.41  10.2    9  3.9  0.00  0.01  0.00  0.00     2
 21 JUN   56 Floral Ini     329   0.55  11.1   12  3.6  0.00  0.00  0.00  0.00     3
 23 SEP  150 75% Silkin    6162   2.88  23.2   95  1.5  0.00  0.04  0.00  0.00     4
 18 OCT  175 Beg Gr Fil   12574   1.99  23.2   99  0.8  0.03  0.29  0.00  0.00     5
 25 JAN  274 End Gr Fil   15316   0.06  23.2   87  0.6  0.71  0.53  0.00  0.00     6
 31 JAN  280 Maturity     15316   0.06  23.2   87  0.6  0.00  0.75  0.00  0.00    10
 31 JAN  280 Harvest      15316   0.06  23.2   87  0.6  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     150          -99
      Physiological maturity day (dap)                       280          -99
      Yield at harvest maturity (kg [dm]/ha)                5465          -99
      Number at maturity (no/m2)                            1503          -99
      Unit wt at maturity (g [dm]/unit)                   0.3636          -99
      Number at maturity (no/unit)                         375.7          -99
      Tops weight at maturity (kg [dm]/ha)                 15316          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    9906          -99
      Leaf area index, maximum                              2.95          -99
      Harvest index at maturity                            0.357          -99
      Grain N at maturity (kg/ha)                             59          -99
      Tops N at maturity (kg/ha)                              87          -99
      Stem N at maturity (kg/ha)                              27          -99
      Grain N at maturity (%)                                1.1          -99
      Tops weight at anthesis (kg [dm]/ha)                  5817          -99
      Tops N at anthesis (kg/ha)                              95          -99
      Leaf number per stem at maturity                     23.23          -99
      Emergence day (dap)                                     10          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    41  18.9  10.3  14.6  20.6  12.01  380.0  151.5   70.8  185.8    0    0    0    0    0   14  0.000  0.000  0.003  0.007  0.000  0.000
 End Juvenil-Floral Init    5  18.7  10.2  14.4  17.9  12.01  380.0    0.4    6.1   18.9    0    0    0    0    0    2  0.000  0.000  0.000  0.001  0.000  0.000
 Floral Init-End Lf Grow   94  18.0   9.8  13.9  18.4  12.01  380.0  519.3  303.0  338.9    0    0    0    0    0   75  0.000  0.000  0.014  0.035  0.000  0.000
 End Lf Grth-Beg Grn Fil   25  19.5  10.1  14.8  22.2  12.00  380.0   25.2  108.1  108.8    0    0    0    0    0   24  0.000  0.019  0.114  0.284  0.000  0.000
 Grain Filling Phase       99  20.3  10.2  15.2  22.5  11.99  380.0   47.3   83.3  479.1    0    0    0    0    0   45  0.614  0.713  0.261  0.528  0.000  0.000
   
 Planting to Harvest      280  19.2  10.1  14.7  20.8  12.00  380.0  779.9  597.1 1215.9    0    0    0    0    0  167  0.217  0.254  0.118  0.241  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 280 days 

 Precipitation during growing season       779.9 mm[rain]
   Dry Matter Productivity                  1.96 kg[DM]/m3[rain]          =   19.6 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.70 kg[grain yield]/m3[rain] =    7.0 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  597.1 mm[ET]
   Dry Matter Productivity                  2.57 kg[DM]/m3[ET]            =   25.7 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.92 kg[grain yield]/m3[ET]   =    9.2 kg[yield]/ha per mm[ET]

 Transpiration during growing season       385.0 mm[EP]
   Dry Matter Productivity                  3.98 kg[DM]/m3[EP]            =   39.8 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.42 kg[grain yield]/m3[EP]   =   14.2 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                 102.1 kg[DM]/kg[N applied]
   Yield Productivity                       36.4 kg[yield]/kg[N applied]

 N uptake during growing season              193 kg[N uptake]/ha
   Dry Matter Productivity                  79.4 kg[DM]/kg[N uptake]
   Yield Productivity                       28.3 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     5465 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:40

*RUN  45        : SAMPLE 45                 MZCER048 EXPEFILE   45
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 45   : SAMPLE 45                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEBS   2021
 SOIL           : IB00000016     TEXTURE : ClayL -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:246.4mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.207 0.330 0.424 0.123 0.330   1.00   1.18   5.85   0.00   0.00   2.55
  5- 15 0.225 0.350 0.434 0.125 0.350   0.85   1.21   5.92   0.00   0.00   2.16
 15- 30 0.226 0.350 0.433 0.124 0.350   0.70   1.22   5.88   0.00   0.00   1.58
 30- 45 0.247 0.372 0.446 0.125 0.372   0.50   1.28   5.99   0.00   0.00   1.01
 45- 60 0.247 0.372 0.446 0.125 0.372   0.50   1.28   5.99   0.00   0.00   1.01
 60- 80 0.247 0.371 0.445 0.124 0.371   0.38   1.35   6.13   0.00   0.00   0.59
 80-100 0.247 0.371 0.445 0.124 0.371   0.38   1.35   6.13   0.00   0.00   0.59
100-150 0.232 0.354 0.434 0.122 0.354   0.05   1.42   6.30   0.00   0.00   0.33
150-200 0.232 0.354 0.434 0.122 0.354   0.05   1.42   6.30   0.00   0.00   0.33

TOT-200  47.2  71.8  87.5  24.6  71.8  <--cm   -  kg/ha-->    0.0    0.0 187599
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    45     SAMPLE 45               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  4 MAY    8 Emergence       16   0.00   1.9    1  4.4  0.00  0.00  0.00  0.00     1
  6 JUN   41 End Juveni     248   0.44  10.4    9  3.8  0.00  0.01  0.00  0.00     2
 11 JUN   46 Floral Ini     389   0.63  11.5   14  3.6  0.00  0.00  0.00  0.00     3
 24 AUG  120 75% Silkin    6576   3.42  23.9  110  1.7  0.01  0.02  0.00  0.00     4
 14 SEP  141 Beg Gr Fil   12665   3.02  23.9  115  0.9  0.00  0.12  0.00  0.00     5
 29 NOV  217 End Gr Fil   18015   0.34  23.9  112  0.6  0.55  0.47  0.00  0.00     6
  4 DEC  222 Maturity     18015   0.34  23.9  112  0.6  0.76  0.80  0.00  0.00    10
  4 DEC  222 Harvest      18015   0.34  23.9  112  0.6  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     120          -99
      Physiological maturity day (dap)                       222          -99
      Yield at harvest maturity (kg [dm]/ha)                6665          -99
      Number at maturity (no/m2)                            1970          -99
      Unit wt at maturity (g [dm]/unit)                   0.3383          -99
      Number at maturity (no/unit)                         492.5          -99
      Tops weight at maturity (kg [dm]/ha)                 18015          -99
      By-product produced (stalk) at maturity (kg[dm]/ha   11407          -99
      Leaf area index, maximum                              3.44          -99
      Harvest index at maturity                            0.370          -99
      Grain N at maturity (kg/ha)                             81          -99
      Tops N at maturity (kg/ha)                             112          -99
      Stem N at maturity (kg/ha)                              31          -99
      Grain N at maturity (%)                                1.2          -99
      Tops weight at anthesis (kg [dm]/ha)                  6195          -99
      Tops N at anthesis (kg/ha)                             110          -99
      Leaf number per stem at maturity                     23.92          -99
      Emergence day (dap)                                      8          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    33  20.9  12.2  16.5  20.3  12.02  380.0  166.7   75.7  154.3    0    0    0    0    0   15  0.000  0.000  0.002  0.006  0.000  0.000
 End Juvenil-Floral Init    5  21.9  11.1  16.5  22.7  12.02  380.0    0.0    7.6   25.1    0    0    0    0    0    0  0.000  0.000  0.000  0.001  0.000  0.000
 Floral Init-End Lf Grow   74  20.0  11.3  15.7  17.9  12.02  380.0  443.5  224.6  269.4    0    0    0    0    0   54  0.000  0.009  0.007  0.018  0.000  0.000
 End Lf Grth-Beg Grn Fil   21  21.1  11.3  16.2  21.1  12.01  380.0   52.2   88.4   88.9    0    0    0    0    0   15  0.000  0.000  0.047  0.118  0.000  0.000
 Grain Filling Phase       76  22.8  11.9  17.4  22.6  11.99  380.0   58.5  168.7  369.4    0    0    0    0    0   55  0.468  0.535  0.234  0.458  0.000  0.000
   
 Planting to Harvest      222  21.4  11.7  16.6  20.5  12.01  380.0  747.6  589.0  972.3    0    0    0    0    0  147  0.175  0.204  0.100  0.193  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 222 days 

 Precipitation during growing season       747.6 mm[rain]
   Dry Matter Productivity                  2.41 kg[DM]/m3[rain]          =   24.1 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.89 kg[grain yield]/m3[rain] =    8.9 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  589.0 mm[ET]
   Dry Matter Productivity                  3.06 kg[DM]/m3[ET]            =   30.6 kg[DM]/ha per mm[ET]
   Yield Productivity                       1.13 kg[grain yield]/m3[ET]   =   11.3 kg[yield]/ha per mm[ET]

 Transpiration during growing season       424.6 mm[EP]
   Dry Matter Productivity                  4.24 kg[DM]/m3[EP]            =   42.4 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.57 kg[grain yield]/m3[EP]   =   15.7 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                 120.1 kg[DM]/kg[N applied]
   Yield Productivity                       44.4 kg[yield]/kg[N applied]

 N uptake during growing season              206 kg[N uptake]/ha
   Dry Matter Productivity                  87.4 kg[DM]/kg[N uptake]
   Yield Productivity                       32.4 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     6665 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:40

*RUN  46        : SAMPLE 46                 MZCER048 EXPEFILE   46
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 46   : SAMPLE 46                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEBT   2021
 SOIL           : IB00000021     TEXTURE : ClayL -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:247.3mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.201 0.324 0.421 0.123 0.324   1.00   1.21   6.12   0.00   0.00   2.22
  5- 15 0.216 0.340 0.429 0.124 0.340   0.85   1.23   6.20   0.00   0.00   1.87
 15- 30 0.217 0.341 0.429 0.124 0.341   0.70   1.24   6.16   0.00   0.00   1.32
 30- 45 0.234 0.359 0.438 0.125 0.359   0.50   1.29   6.25   0.00   0.00   0.85
 45- 60 0.234 0.359 0.438 0.125 0.359   0.50   1.29   6.25   0.00   0.00   0.85
 60- 80 0.234 0.358 0.437 0.124 0.358   0.38   1.35   6.39   0.00   0.00   0.49
 80-100 0.234 0.358 0.437 0.124 0.358   0.38   1.35   6.39   0.00   0.00   0.49
100-150 0.221 0.344 0.429 0.123 0.344   0.05   1.41   6.57   0.00   0.00   0.28
150-200 0.221 0.344 0.429 0.123 0.344   0.05   1.41   6.57   0.00   0.00   0.28

TOT-200  44.9  69.6  86.3  24.7  69.6  <--cm   -  kg/ha-->    0.0    0.0 159819
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    46     SAMPLE 46               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 27 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 28 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
 14 MAY   17 Emergence       16   0.00   1.7    1  4.4  0.00  0.00  0.00  0.00     1
  2 AUG   97 End Juveni     199   0.36  10.1    7  3.6  0.00  0.01  0.00  0.00     2
  7 AUG  102 Floral Ini     234   0.40  10.5    7  3.1  0.00  0.12  0.00  0.00     3
  3 JAN  251 75% Silkin    1742   0.13  22.2   16  0.9  0.00  0.65  0.00  0.00     4
 13 FEB  292 Beg Gr Fil    1780   0.05  22.2   20  1.1  0.00  0.46  0.00  0.00     5
 27 APR  365 Harvest       2587   0.04  22.2   30  1.2  0.00  0.06  0.00  0.00     5


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     251          -99
      Physiological maturity day (dap)                       -99          -99
      Yield at harvest maturity (kg [dm]/ha)                 867          -99
      Number at maturity (no/m2)                             281          -99
      Unit wt at maturity (g [dm]/unit)                   0.3086          -99
      Number at maturity (no/unit)                          71.4          -99
      Tops weight at maturity (kg [dm]/ha)                  2587          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    1742          -99
      Leaf area index, maximum                              0.43          -99
      Harvest index at maturity                            0.335          -99
      Grain N at maturity (kg/ha)                             14          -99
      Tops N at maturity (kg/ha)                              30          -99
      Stem N at maturity (kg/ha)                              16          -99
      Grain N at maturity (%)                                1.6          -99
      Tops weight at anthesis (kg [dm]/ha)                  1726          -99
      Tops N at anthesis (kg/ha)                              16          -99
      Leaf number per stem at maturity                     22.18          -99
      Emergence day (dap)                                     17          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    80  15.1   7.2  11.2  19.0  12.06  380.0   1691    220    312    0    0    0    0    0   73  0.000  0.000  0.003  0.007  0.000  0.000
 End Juvenil-Floral Init    5  14.6   6.4  10.5  20.0  12.05  380.0   77.4   19.7   19.7    0    0    0    0    0    5  0.000  0.000  0.046  0.114  0.000  0.000
 Floral Init-End Lf Grow  149  15.5   7.5  11.5  21.4  11.98  380.0   3750    504    647    0    0    0    0    0  133  0.000  0.000  0.392  0.644  0.000  0.000
 End Lf Grth-Beg Grn Fil   41  16.8   7.2  12.0  23.4  11.95  380.0  250.3   56.3  199.9    0    0    0    0    0   23  0.000  0.000  0.215  0.471  0.000  0.000
 Grain Filling Phase       74  17.5   8.1  12.8  24.3  12.00  380.0  693.6  149.4  383.6    0    0    0    0    0   50  0.000  0.000  0.026  0.064  0.000  0.000
   
 Planting to Harvest      365  16.0   7.5  11.8  21.5  12.00  380.0   7043    997   1626    0    0    0    0    0  299  0.000  0.000  0.191  0.332  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 365 days 

 Precipitation during growing season      7042.9 mm[rain]
   Dry Matter Productivity                  0.04 kg[DM]/m3[rain]          =    0.4 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.01 kg[grain yield]/m3[rain] =    0.1 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  997.5 mm[ET]
   Dry Matter Productivity                  0.26 kg[DM]/m3[ET]            =    2.6 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.09 kg[grain yield]/m3[ET]   =    0.9 kg[yield]/ha per mm[ET]

 Transpiration during growing season       145.8 mm[EP]
   Dry Matter Productivity                  1.77 kg[DM]/m3[EP]            =   17.7 kg[DM]/ha per mm[EP]
   Yield Productivity                       0.59 kg[grain yield]/m3[EP]   =    5.9 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                  17.2 kg[DM]/kg[N applied]
   Yield Productivity                        5.8 kg[yield]/kg[N applied]

 N uptake during growing season               51 kg[N uptake]/ha
   Dry Matter Productivity                  50.7 kg[DM]/kg[N uptake]
   Yield Productivity                       17.0 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :      867 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:40

*RUN  47        : SAMPLE 47                 MZCER048 EXPEFILE   47
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 47   : SAMPLE 47                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEBU   2021
 SOIL           : IB00000011     TEXTURE : yLoam -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:247.6mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.217 0.342 0.430 0.125 0.342   1.00   1.20   5.99   0.00   0.00   2.16
  5- 15 0.229 0.354 0.436 0.125 0.354   0.85   1.22   6.06   0.00   0.00   1.83
 15- 30 0.231 0.356 0.437 0.125 0.356   0.70   1.23   6.02   0.00   0.00   1.32
 30- 45 0.243 0.368 0.443 0.125 0.368   0.50   1.28   6.15   0.00   0.00   0.85
 45- 60 0.243 0.368 0.443 0.125 0.368   0.50   1.28   6.15   0.00   0.00   0.85
 60- 80 0.244 0.368 0.443 0.124 0.368   0.38   1.35   6.31   0.00   0.00   0.49
 80-100 0.244 0.368 0.443 0.124 0.368   0.38   1.35   6.31   0.00   0.00   0.49
100-150 0.235 0.358 0.436 0.123 0.358   0.05   1.41   6.51   0.00   0.00   0.28
150-200 0.235 0.358 0.436 0.123 0.358   0.05   1.41   6.51   0.00   0.00   0.28

TOT-200  47.4  72.2  87.7  24.8  72.2  <--cm   -  kg/ha-->    0.0    0.0 158220
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    47     SAMPLE 47               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  4 MAY    8 Emergence       16   0.00   1.9    1  4.4  0.00  0.00  0.00  0.00     1
  3 JUN   38 End Juveni     226   0.41  10.1    8  3.7  0.00  0.00  0.00  0.00     2
  8 JUN   43 Floral Ini     381   0.62  11.5   14  3.6  0.00  0.00  0.00  0.00     3
 15 AUG  111 75% Silkin    6291   3.26  23.8  106  1.7  0.01  0.03  0.00  0.00     4
  3 SEP  130 Beg Gr Fil   11973   2.84  23.8  109  0.9  0.00  0.15  0.00  0.00     5
 18 NOV  206 End Gr Fil   19885   0.37  23.8  121  0.6  0.00  0.54  0.00  0.00     6
 23 NOV  211 Maturity     19885   0.37  23.8  121  0.6  0.00  0.85  0.00  0.00    10
 23 NOV  211 Harvest      19885   0.37  23.8  121  0.6  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     111          -99
      Physiological maturity day (dap)                       211          -99
      Yield at harvest maturity (kg [dm]/ha)                9046          -99
      Number at maturity (no/m2)                            1989          -99
      Unit wt at maturity (g [dm]/unit)                   0.4549          -99
      Number at maturity (no/unit)                         497.2          -99
      Tops weight at maturity (kg [dm]/ha)                 19885          -99
      By-product produced (stalk) at maturity (kg[dm]/ha   10896          -99
      Leaf area index, maximum                              3.27          -99
      Harvest index at maturity                            0.455          -99
      Grain N at maturity (kg/ha)                             91          -99
      Tops N at maturity (kg/ha)                             121          -99
      Stem N at maturity (kg/ha)                              30          -99
      Grain N at maturity (%)                                1.0          -99
      Tops weight at anthesis (kg [dm]/ha)                  6021          -99
      Tops N at anthesis (kg/ha)                             106          -99
      Leaf number per stem at maturity                     23.82          -99
      Emergence day (dap)                                      8          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    30  20.9  13.4  17.2  21.0  12.06  380.0  182.4   68.0  147.1    0    0    0    0    0   16  0.000  0.000  0.002  0.006  0.000  0.000
 End Juvenil-Floral Init    5  22.4  13.2  17.8  21.8  12.07  380.0    0.1    8.0   24.8    0    0    0    0    0    1  0.000  0.000  0.000  0.000  0.000  0.000
 Floral Init-End Lf Grow   68  20.2  12.7  16.4  18.4  12.06  380.0  644.0  210.1  258.5    0    0    0    0    0   46  0.000  0.006  0.013  0.032  0.000  0.000
 End Lf Grth-Beg Grn Fil   19  21.1  12.5  16.8  21.9  12.03  380.0   60.3   84.0   84.5    0    0    0    0    0   14  0.000  0.000  0.057  0.142  0.000  0.000
 Grain Filling Phase       76  21.8  12.9  17.3  22.6  11.98  380.0  260.5  275.1  369.1    0    0    0    0    0   40  0.000  0.000  0.303  0.532  0.000  0.000
   
 Planting to Harvest      211  21.2  12.9  17.0  21.0  12.02  380.0   1184    657    956    0    0    0    0    0  122  0.000  0.002  0.133  0.236  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 211 days 

 Precipitation during growing season      1184.4 mm[rain]
   Dry Matter Productivity                  1.68 kg[DM]/m3[rain]          =   16.8 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.76 kg[grain yield]/m3[rain] =    7.6 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  656.6 mm[ET]
   Dry Matter Productivity                  3.03 kg[DM]/m3[ET]            =   30.3 kg[DM]/ha per mm[ET]
   Yield Productivity                       1.38 kg[grain yield]/m3[ET]   =   13.8 kg[yield]/ha per mm[ET]

 Transpiration during growing season       494.1 mm[EP]
   Dry Matter Productivity                  4.02 kg[DM]/m3[EP]            =   40.2 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.83 kg[grain yield]/m3[EP]   =   18.3 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                 132.6 kg[DM]/kg[N applied]
   Yield Productivity                       60.3 kg[yield]/kg[N applied]

 N uptake during growing season              205 kg[N uptake]/ha
   Dry Matter Productivity                  97.0 kg[DM]/kg[N uptake]
   Yield Productivity                       44.1 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     9046 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:40

*RUN  48        : SAMPLE 48                 MZCER048 EXPEFILE   48
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 48   : SAMPLE 48                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEBV   2021
 SOIL           : IB00000009     TEXTURE : yLoam -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:253.8mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.217 0.343 0.432 0.126 0.343   1.00   1.14   6.01   0.00   0.00   2.35
  5- 15 0.229 0.356 0.438 0.127 0.356   0.85   1.17   6.08   0.00   0.00   1.99
 15- 30 0.233 0.361 0.441 0.128 0.361   0.70   1.18   6.07   0.00   0.00   1.44
 30- 45 0.246 0.374 0.449 0.128 0.374   0.50   1.23   6.18   0.00   0.00   0.92
 45- 60 0.246 0.374 0.449 0.128 0.374   0.50   1.23   6.18   0.00   0.00   0.92
 60- 80 0.246 0.374 0.448 0.128 0.374   0.38   1.31   6.31   0.00   0.00   0.54
 80-100 0.246 0.374 0.448 0.128 0.374   0.38   1.31   6.31   0.00   0.00   0.54
100-150 0.237 0.363 0.441 0.126 0.363   0.05   1.36   6.50   0.00   0.00   0.31
150-200 0.237 0.363 0.441 0.126 0.363   0.05   1.36   6.50   0.00   0.00   0.31

TOT-200  47.8  73.2  88.6  25.4  73.2  <--cm   -  kg/ha-->    0.0    0.0 166570
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    48     SAMPLE 48               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  4 MAY    8 Emergence       16   0.00   1.9    1  4.4  0.00  0.00  0.00  0.00     1
  6 JUN   41 End Juveni     248   0.44  10.4    9  3.8  0.00  0.01  0.00  0.00     2
 11 JUN   46 Floral Ini     389   0.63  11.5   14  3.6  0.00  0.00  0.00  0.00     3
 24 AUG  120 75% Silkin    6561   3.35  23.9  108  1.6  0.00  0.03  0.00  0.00     4
 14 SEP  141 Beg Gr Fil   12510   2.87  23.9  113  0.9  0.00  0.15  0.00  0.00     5
 29 NOV  217 End Gr Fil   17860   0.32  23.9  109  0.6  0.53  0.49  0.00  0.00     6
  4 DEC  222 Maturity     17860   0.32  23.9  109  0.6  0.76  0.81  0.00  0.00    10
  4 DEC  222 Harvest      17860   0.32  23.9  109  0.6  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     120          -99
      Physiological maturity day (dap)                       222          -99
      Yield at harvest maturity (kg [dm]/ha)                6624          -99
      Number at maturity (no/m2)                            1935          -99
      Unit wt at maturity (g [dm]/unit)                   0.3423          -99
      Number at maturity (no/unit)                         483.8          -99
      Tops weight at maturity (kg [dm]/ha)                 17860          -99
      By-product produced (stalk) at maturity (kg[dm]/ha   11295          -99
      Leaf area index, maximum                              3.37          -99
      Harvest index at maturity                            0.371          -99
      Grain N at maturity (kg/ha)                             78          -99
      Tops N at maturity (kg/ha)                             109          -99
      Stem N at maturity (kg/ha)                              31          -99
      Grain N at maturity (%)                                1.2          -99
      Tops weight at anthesis (kg [dm]/ha)                  6186          -99
      Tops N at anthesis (kg/ha)                             108          -99
      Leaf number per stem at maturity                     23.92          -99
      Emergence day (dap)                                      8          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    33  20.9  12.2  16.5  20.3  12.02  380.0  166.7   76.2  154.3    0    0    0    0    0   15  0.000  0.000  0.002  0.006  0.000  0.000
 End Juvenil-Floral Init    5  21.9  11.1  16.5  22.7  12.02  380.0    0.0    7.7   25.2    0    0    0    0    0    0  0.000  0.000  0.000  0.001  0.000  0.000
 Floral Init-End Lf Grow   74  20.0  11.3  15.7  17.9  12.02  380.0  443.5  224.7  269.5    0    0    0    0    0   54  0.000  0.003  0.010  0.026  0.000  0.000
 End Lf Grth-Beg Grn Fil   21  21.1  11.3  16.2  21.1  12.01  380.0   52.2   88.5   89.0    0    0    0    0    0   15  0.000  0.000  0.057  0.144  0.000  0.000
 Grain Filling Phase       76  22.8  11.9  17.4  22.6  11.99  380.0   58.5  172.2  370.8    0    0    0    0    0   55  0.452  0.518  0.251  0.483  0.000  0.000
   
 Planting to Harvest      222  21.4  11.7  16.6  20.5  12.01  380.0  747.6  593.5  974.0    0    0    0    0    0  147  0.169  0.195  0.108  0.207  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 222 days 

 Precipitation during growing season       747.6 mm[rain]
   Dry Matter Productivity                  2.39 kg[DM]/m3[rain]          =   23.9 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.89 kg[grain yield]/m3[rain] =    8.9 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  593.5 mm[ET]
   Dry Matter Productivity                  3.01 kg[DM]/m3[ET]            =   30.1 kg[DM]/ha per mm[ET]
   Yield Productivity                       1.12 kg[grain yield]/m3[ET]   =   11.2 kg[yield]/ha per mm[ET]

 Transpiration during growing season       423.5 mm[EP]
   Dry Matter Productivity                  4.22 kg[DM]/m3[EP]            =   42.2 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.56 kg[grain yield]/m3[EP]   =   15.6 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                 119.1 kg[DM]/kg[N applied]
   Yield Productivity                       44.2 kg[yield]/kg[N applied]

 N uptake during growing season              198 kg[N uptake]/ha
   Dry Matter Productivity                  90.2 kg[DM]/kg[N uptake]
   Yield Productivity                       33.5 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     6624 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:40

*RUN  49        : SAMPLE 49                 MZCER048 EXPEFILE   49
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 49   : SAMPLE 49                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEBW   2021
 SOIL           : IB00000018     TEXTURE : yLoam -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:253.6mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.213 0.341 0.431 0.128 0.341   1.00   1.21   6.12   0.00   0.00   1.86
  5- 15 0.225 0.353 0.437 0.128 0.353   0.85   1.23   6.20   0.00   0.00   1.58
 15- 30 0.229 0.357 0.439 0.128 0.357   0.70   1.23   6.20   0.00   0.00   1.13
 30- 45 0.243 0.371 0.447 0.128 0.371   0.50   1.28   6.31   0.00   0.00   0.73
 45- 60 0.243 0.371 0.447 0.128 0.371   0.50   1.28   6.31   0.00   0.00   0.73
 60- 80 0.243 0.370 0.446 0.127 0.370   0.38   1.35   6.44   0.00   0.00   0.42
 80-100 0.243 0.370 0.446 0.127 0.370   0.38   1.35   6.44   0.00   0.00   0.42
100-150 0.233 0.359 0.439 0.126 0.359   0.05   1.40   6.61   0.00   0.00   0.24
150-200 0.233 0.359 0.439 0.126 0.359   0.05   1.40   6.61   0.00   0.00   0.24

TOT-200  47.1  72.4  88.3  25.4  72.4  <--cm   -  kg/ha-->    0.0    0.0 135848
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    49     SAMPLE 49               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 26 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 27 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  4 MAY    8 Emergence       16   0.00   1.9    1  4.4  0.00  0.00  0.00  0.00     1
  4 JUN   39 End Juveni     243   0.43  10.3    9  3.8  0.00  0.00  0.00  0.00     2
  9 JUN   44 Floral Ini     388   0.63  11.5   14  3.6  0.00  0.00  0.00  0.00     3
 18 AUG  114 75% Silkin    6462   3.22  24.1  108  1.7  0.01  0.04  0.00  0.00     4
  7 SEP  134 Beg Gr Fil   12137   2.75  24.1  111  0.9  0.00  0.16  0.00  0.00     5
 20 NOV  208 End Gr Fil   17819   0.34  24.1  109  0.6  0.48  0.52  0.00  0.00     6
 25 NOV  213 Maturity     17819   0.34  24.1  109  0.6  0.81  0.81  0.00  0.00    10
 25 NOV  213 Harvest      17819   0.34  24.1  109  0.6  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     114          -99
      Physiological maturity day (dap)                       213          -99
      Yield at harvest maturity (kg [dm]/ha)                6708          -99
      Number at maturity (no/m2)                            1930          -99
      Unit wt at maturity (g [dm]/unit)                   0.3475          -99
      Number at maturity (no/unit)                         482.6          -99
      Tops weight at maturity (kg [dm]/ha)                 17819          -99
      By-product produced (stalk) at maturity (kg[dm]/ha   11169          -99
      Leaf area index, maximum                              3.23          -99
      Harvest index at maturity                            0.376          -99
      Grain N at maturity (kg/ha)                             76          -99
      Tops N at maturity (kg/ha)                             109          -99
      Stem N at maturity (kg/ha)                              32          -99
      Grain N at maturity (%)                                1.1          -99
      Tops weight at anthesis (kg [dm]/ha)                  6112          -99
      Tops N at anthesis (kg/ha)                             108          -99
      Leaf number per stem at maturity                     24.07          -99
      Emergence day (dap)                                      8          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    31  21.4  12.6  17.0  20.4  12.01  380.0  166.7   76.1  147.5    0    0    0    0    0   15  0.000  0.000  0.002  0.005  0.000  0.000
 End Juvenil-Floral Init    5  22.2  11.8  17.0  22.2  12.01  380.0    0.0    7.6   24.8    0    0    0    0    0    0  0.000  0.000  0.001  0.001  0.000  0.000
 Floral Init-End Lf Grow   70  20.5  11.8  16.1  18.1  12.01  380.0  442.2  211.8  260.0    0    0    0    0    0   50  0.000  0.005  0.017  0.044  0.000  0.000
 End Lf Grth-Beg Grn Fil   20  21.5  11.6  16.5  21.4  12.00  380.0   49.0   86.3   86.8    0    0    0    0    0   14  0.000  0.000  0.062  0.155  0.000  0.000
 Grain Filling Phase       74  23.0  12.3  17.6  22.5  12.00  380.0   59.0  186.4  361.1    0    0    0    0    0   56  0.395  0.466  0.273  0.507  0.000  0.000
   
 Planting to Harvest      213  21.8  12.1  17.0  20.6  12.00  380.0  738.2  586.3  950.7    0    0    0    0    0  141  0.154  0.183  0.120  0.225  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 213 days 

 Precipitation during growing season       738.2 mm[rain]
   Dry Matter Productivity                  2.41 kg[DM]/m3[rain]          =   24.1 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.91 kg[grain yield]/m3[rain] =    9.1 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  586.3 mm[ET]
   Dry Matter Productivity                  3.04 kg[DM]/m3[ET]            =   30.4 kg[DM]/ha per mm[ET]
   Yield Productivity                       1.14 kg[grain yield]/m3[ET]   =   11.4 kg[yield]/ha per mm[ET]

 Transpiration during growing season       416.3 mm[EP]
   Dry Matter Productivity                  4.28 kg[DM]/m3[EP]            =   42.8 kg[DM]/ha per mm[EP]
   Yield Productivity                       1.61 kg[grain yield]/m3[EP]   =   16.1 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                 118.8 kg[DM]/kg[N applied]
   Yield Productivity                       44.7 kg[yield]/kg[N applied]

 N uptake during growing season              187 kg[N uptake]/ha
   Dry Matter Productivity                  95.3 kg[DM]/kg[N uptake]
   Yield Productivity                       35.9 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     6708 kg/ha    [Dry weight] 

**************************************************************************************************************

*DSSAT Cropping System Model Ver. 4.8.2.000 -release  JUN 13, 2024 20:54:40

*RUN  50        : SAMPLE 50                 MZCER048 EXPEFILE   50
 MODEL          : MZCER048 - Maize
 EXPERIMENT     : EXPEFILE MZ SPATIAL ANALYSES TEST CASE; FLORENCE, SOUTH CAROLI
 DATA PATH      : /tmp/dssatrun7XN8CCF9/
 TREATMENT 50   : SAMPLE 50                 MZCER048


 CROP           : Maize            CULTIVAR : SOTUBAKA         ECOTYPE :IB0001
 STARTING DATE  : APR  1 2021
 PLANTING DATE  : AUTOMATIC PLANTING PLANTS/m2 :  4.0     ROW SPACING :  90.cm
 WEATHER        : SEBX   2021
 SOIL           : IB00000006     TEXTURE : ClayL -    ISRIC soilgrids + HC27
 SOIL INIT COND : DEPTH:200cm EXTR. H2O:243.3mm  NO3:  0.0kg/ha  NH4:  0.0kg/ha
 WATER BALANCE  : RAINFED
 IRRIGATION     : NOT IRRIGATED
 NITROGEN BAL.  : SOIL-N & N-UPTAKE SIMULATION; NO N-FIXATION
 N-FERTILIZER   :      150 kg/ha IN     3 APPLICATIONS
 RESIDUE/MANURE : INITIAL :     0 kg/ha ;       0 kg/ha IN     0 APPLICATIONS
 ENVIRONM. OPT. : DAYL=    0.00  SRAD=    0.00  TMAX=    0.00  TMIN=    0.00
                  RAIN=    0.00  CO2 =    0.00  DEW =    0.00  WIND=    0.00
 SIMULATION OPT : WATER   :Y  NITROGEN:Y  N-FIX:N  PHOSPH :N  PESTS  :N
                  PHOTO   :C  ET      :R  INFIL:S  HYDROL :R  SOM    :G
                  CO2     :D  NSWIT   :1  EVAP :R  SOIL   :2  STEMP  :D
 MANAGEMENT OPT : PLANTING:F  IRRIG   :N  FERT :D  RESIDUE:N  HARVEST:M
                  WEATHER :M  TILLAGE :N

*SUMMARY OF SOIL AND GENETIC INPUT PARAMETERS

   SOIL LOWER UPPER   SAT  EXTR  INIT   ROOT   BULK     pH    NO3    NH4    ORG
  DEPTH LIMIT LIMIT    SW    SW    SW   DIST   DENS                          C
   cm   cm3/cm3    cm3/cm3    cm3/cm3         g/cm3         ugN/g  ugN/g     %
-------------------------------------------------------------------------------
  0-  5 0.203 0.325 0.422 0.122 0.325   1.00   1.23   6.19   0.00   0.00   1.96
  5- 15 0.219 0.343 0.430 0.124 0.343   0.85   1.25   6.25   0.00   0.00   1.64
 15- 30 0.219 0.342 0.429 0.123 0.342   0.70   1.26   6.21   0.00   0.00   1.15
 30- 45 0.236 0.360 0.438 0.124 0.360   0.50   1.31   6.32   0.00   0.00   0.74
 45- 60 0.236 0.360 0.438 0.124 0.360   0.50   1.31   6.32   0.00   0.00   0.74
 60- 80 0.236 0.359 0.437 0.123 0.359   0.38   1.37   6.45   0.00   0.00   0.43
 80-100 0.236 0.359 0.437 0.123 0.359   0.38   1.37   6.45   0.00   0.00   0.43
100-150 0.223 0.343 0.429 0.120 0.343   0.05   1.42   6.63   0.00   0.00   0.24
150-200 0.223 0.343 0.429 0.120 0.343   0.05   1.42   6.63   0.00   0.00   0.24

TOT-200  45.3  69.6  86.4  24.3  69.6  <--cm   -  kg/ha-->    0.0    0.0 141015
SOIL ALBEDO    : 0.10      EVAPORATION LIMIT : 6.00         MIN. FACTOR  : 1.00
RUNOFF CURVE # :75.00      DRAINAGE RATE     : 0.50         FERT. FACTOR : 1.00

 Maize            CULTIVAR: IM0001-SOTUBAKA           ECOTYPE: IB0001
 P1     : 300.00  P2     : 0.5200  P5     : 930.00
 G2     : 500.00  G3     :  6.000  PHINT  : 38.900


*SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES

 RUN NO.    50     SAMPLE 50               

        CROP GROWTH     BIOMASS         LEAF   CROP N     STRESS      STRESS                                           
   DATE  AGE STAGE        kg/ha    LAI   NUM  kg/ha  %   H2O  Nitr Phos1 Phos2  RSTG                                   
 ------  --- ----------   -----  -----  ----  ---  ---  ----  ----  ----  ----  ----                                   
  1 APR    0 Start Sim        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     7
 27 APR    0 Sowing           0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     8
 28 APR    1 Germinate        0   0.00   0.0    0  0.0  0.00  0.00  0.00  0.00     9
  8 MAY   11 Emergence       16   0.00   1.8    1  4.4  0.00  0.00  0.00  0.00     1
 20 JUN   54 End Juveni     220   0.40  10.1    9  3.9  0.00  0.00  0.00  0.00     2
 25 JUN   59 Floral Ini     303   0.51  10.9   11  3.6  0.00  0.00  0.00  0.00     3
  1 OCT  157 75% Silkin    3301   0.58  22.9   30  0.9  0.00  0.35  0.00  0.00     4
 27 OCT  183 Beg Gr Fil    3927   0.23  22.9   31  0.8  0.00  0.68  0.00  0.00     5
  1 FEB  280 End Gr Fil    5042   0.02  22.9   40  0.8  0.00  0.48  0.00  0.00     6
  7 FEB  286 Maturity      5042   0.02  22.9   40  0.8  0.00  0.23  0.00  0.00    10
  7 FEB  286 Harvest       5042   0.02  22.9   40  0.8  0.00  0.00  0.00  0.00    10


*MAIN GROWTH AND DEVELOPMENT VARIABLES

@     VARIABLE                                         SIMULATED     MEASURED
      --------                                         ---------     --------
      Anthesis day (dap)                                     157          -99
      Physiological maturity day (dap)                       286          -99
      Yield at harvest maturity (kg [dm]/ha)                1448          -99
      Number at maturity (no/m2)                             265          -99
      Unit wt at maturity (g [dm]/unit)                   0.5472          -99
      Number at maturity (no/unit)                          66.2          -99
      Tops weight at maturity (kg [dm]/ha)                  5042          -99
      By-product produced (stalk) at maturity (kg[dm]/ha    3633          -99
      Leaf area index, maximum                              1.42          -99
      Harvest index at maturity                            0.287          -99
      Grain N at maturity (kg/ha)                             16          -99
      Tops N at maturity (kg/ha)                              40          -99
      Stem N at maturity (kg/ha)                              24          -99
      Grain N at maturity (%)                                1.1          -99
      Tops weight at anthesis (kg [dm]/ha)                  3242          -99
      Tops N at anthesis (kg/ha)                              30          -99
      Leaf number per stem at maturity                     22.87          -99
      Emergence day (dap)                                     11          -99


*ENVIRONMENTAL AND STRESS FACTORS

 |-----Development Phase------|--------------------------------------Environment--------------------------------------|-----------------Stress-----------------|
                              |---------------Average--------------|-----Cumulative-----|--------Count of days--------|          (0=Min, 1=Max Stress)         |
                         Time  Temp  Temp  Temp Solar Photop                Evapo    Pot TMIN TMIN TMAX TMAX TMAX RAIN|----Water----|---Nitrogen--|-Phosphorus-|
                         Span   Max   Min  Mean   Rad  [day]    CO2   Rain  Trans     ET   <    <    >    >    >    >   Photo         Photo         Photo
                         days     C     C     C MJ/m2     hr    ppm     mm     mm    mm   0 C  2 C 30 C 32 C 34 C  0mm  synth Growth  synth Growth  synth Growth
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Emergence-End Juvenile    43  18.5   9.9  14.2  20.1  12.05  380.0  588.1  117.4  189.7    0    0    0    0    0   38  0.000  0.000  0.002  0.006  0.000  0.000
 End Juvenil-Floral Init    5  17.8   8.9  13.4  20.8  12.06  380.0    3.8   15.9   21.7    0    0    0    0    0    3  0.000  0.000  0.000  0.001  0.000  0.000
 Floral Init-End Lf Grow   98  17.6   9.6  13.6  18.9  12.03  380.0   3837    369    375    0    0    0    0    0   98  0.000  0.000  0.179  0.345  0.000  0.000
 End Lf Grth-Beg Grn Fil   26  19.0  10.2  14.6  21.5  11.98  380.0   1105    106    121    0    0    0    0    0   26  0.000  0.000  0.418  0.682  0.000  0.000
 Grain Filling Phase       97  20.1  10.5  15.3  22.5  11.95  380.0  440.9  224.4  493.2    0    0    0    0    0   66  0.000  0.000  0.227  0.484  0.000  0.000
   
 Planting to Harvest      286  18.9  10.0  14.5  20.8  12.00  380.0   6488    878   1287    0    0    0    0    0  246  0.000  0.000  0.179  0.350  0.000  0.000
----------------------------------------------------------------------------------------------------------------------------------------------------------------


*Resource Productivity
 Growing season length: 286 days 

 Precipitation during growing season      6487.5 mm[rain]
   Dry Matter Productivity                  0.08 kg[DM]/m3[rain]          =    0.8 kg[DM]/ha per mm[rain]
   Yield Productivity                       0.02 kg[grain yield]/m3[rain] =    0.2 kg[yield]/ha per mm[rain]

 Evapotranspiration during growing season  877.8 mm[ET]
   Dry Matter Productivity                  0.57 kg[DM]/m3[ET]            =    5.7 kg[DM]/ha per mm[ET]
   Yield Productivity                       0.16 kg[grain yield]/m3[ET]   =    1.6 kg[yield]/ha per mm[ET]

 Transpiration during growing season       259.6 mm[EP]
   Dry Matter Productivity                  1.94 kg[DM]/m3[EP]            =   19.4 kg[DM]/ha per mm[EP]
   Yield Productivity                       0.56 kg[grain yield]/m3[EP]   =    5.6 kg[yield]/ha per mm[EP]

 N applied during growing season            150. kg[N applied]/ha
   Dry Matter Productivity                  33.6 kg[DM]/kg[N applied]
   Yield Productivity                        9.7 kg[yield]/kg[N applied]

 N uptake during growing season               91 kg[N uptake]/ha
   Dry Matter Productivity                  55.4 kg[DM]/kg[N uptake]
   Yield Productivity                       15.9 kg[yield]/kg[N uptake]

--------------------------------------------------------------------------------------------------------------

                     Maize YIELD :     1448 kg/ha    [Dry weight] 

**************************************************************************************************************
"""