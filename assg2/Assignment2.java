import java.util.Properties;
import java.util.Scanner;
import java.sql.*;
import java.io.FileInputStream;
import java.util.List;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;

public class Assignment2 {

   public static final int	INTERVAL_RANGE = 60;
   static Connection readConnect = null;
   static Connection writeConnect = null;
   static final String dropPerformanceTable = "drop table if exists Performance";
   static final String createPerformanceTable = "create table " +
   "Performance (Industry char(30), Ticker char(6), StartDate char(10), " +
   "EndDate char(10), TickerReturn char(12), IndustryReturn char(12))";
   static final String insertPerformance = "insert into Performance " +
   "(Industry, Ticker, StartDate, EndDate, TickerReturn, IndustryReturn) " +
   "values(?, ?, ?, ?, ?, ?)";

   public static void main(String[]	args) throws Exception {

      String writeParamsFile = "writerparams.txt";
      String readParamsFile =	"readerparams.txt";
      Properties readProps = new Properties();
      Properties writeProps= new Properties();
  		readProps.load(new FileInputStream(readParamsFile));
      writeProps.load(new FileInputStream(writeParamsFile));

      try {
      		 Class.forName("com.mysql.jdbc.Driver");
      		 String	readDburl =	readProps.getProperty("dburl");
           String writeDburl = writeProps.getProperty("dburl");
      		 String	username = readProps.getProperty("user");
      		 readConnect = DriverManager.getConnection(readDburl, readProps);
           writeConnect = DriverManager.getConnection(writeDburl,writeProps);

           System.out.printf("Database readConnection %s %s established.%n",readDburl,username);
           System.out.printf("Database writeConnection %s %s established.%n",writeDburl,username);

           createPerformanceTable();
           ArrayList<String> distinctIndustries = new ArrayList<String>();
           HashMap<String,List<IntervalDataRange>> tickerToIntervals;
           LinkedList<TickerInfo> priceVolume;
           ArrayList<String> distinctTickers;
           int numDistinctTickers;
           int tradingDayIntervals;
           getDistinctIndustries(distinctIndustries);

           for(int i = 0; i < distinctIndustries.size(); i++){
               System.out.println("processing " + distinctIndustries.get(i));
               String [] range = new String[2];
               tickerToIntervals = new HashMap<String,List<IntervalDataRange>>();
               findDataRange(range,distinctIndustries.get(i));
               tradingDayIntervals = determineTradingDayIntervals(range[0],range[1],distinctIndustries.get(i));
               distinctTickers = distinctTickersInIndustry(range[0],range[1],distinctIndustries.get(i));
               numDistinctTickers = distinctTickers.size();
               if(numDistinctTickers >= 2) {
                  priceVolume =  applyStockSplits(distinctIndustries.get(i),range[0],range[1],distinctTickers);
                  if(priceVolume.size() > 0) {
                      mapTickerToIntervals(tickerToIntervals,priceVolume,tradingDayIntervals,numDistinctTickers);
                      updatePerformanceTable(tickerToIntervals,distinctTickers,tradingDayIntervals,numDistinctTickers,distinctIndustries.get(i));
                  }
              }
              System.out.println();
            }


           readConnect.close();
           writeConnect.close();
           System.out.println("Database connections closed");

    }	catch(SQLException ex) {
  			System.out.printf("SQLException: %s%nSQLSTATE: %s%nVendorError: %s%n",
  			ex.getMessage(),ex.getSQLState(),ex.getErrorCode());
	  }
   }



   static void getDistinctIndustries(ArrayList<String> distinctIndustries) throws Exception{
        Statement stmt = readConnect.createStatement();
        String query = "select distinct (Industry) from Company order by Industry asc";
        ResultSet rs = stmt.executeQuery(query);
        while(rs.next()){
            distinctIndustries.add(rs.getString(1));
        }
        stmt.close();
   }

   static void updatePerformanceTable(HashMap<String,List<IntervalDataRange>> tickersToIntervals,ArrayList<String> distinctTickers, int tradingDayIntervals,int numDistinctTickers, String industry)throws Exception{
        double industryReturn = 0;
        double tickerReturn = 0;
        String industryReturnString;
        String tickerReturnString;
        String currentTicker;
        List<IntervalDataRange> tickerIntervals;
        List<IntervalDataRange> alphabeticallyFirstTickerIntervals = tickersToIntervals.get(distinctTickers.get(0));

         for(int i = 0; i < numDistinctTickers;i++){
            currentTicker = distinctTickers.get(i);
            tickerIntervals = tickersToIntervals.get(currentTicker);
            for(int j = 0; j < tradingDayIntervals; j++){
                tickerReturn = ((tickerIntervals.get(j).lastDayOfInterval.getClosePrice())/(tickerIntervals.get(j).firstDayOfInterval.getOpenPrice())) - 1;
                tickerReturnString = String.format("%10.7f",tickerReturn);
                for(int k = 0; k < distinctTickers.size(); k++){
                    if(i != k){
                        industryReturn = industryReturn  + ((tickersToIntervals.get(distinctTickers.get(k)).get(j).lastDayOfInterval.getClosePrice()/
                        tickersToIntervals.get(distinctTickers.get(k)).get(j).firstDayOfInterval.getOpenPrice()));
                    }
                }
                industryReturn = (industryReturn/(distinctTickers.size() - 1)) - 1.0;
                industryReturnString = String.format("%10.7f",industryReturn);
                insertPerformance(industry,currentTicker,alphabeticallyFirstTickerIntervals.get(j).startDate,
                alphabeticallyFirstTickerIntervals.get(j).endDate,tickerReturnString,industryReturnString);
                industryReturn = 0;
            }
        }
    }

   static void insertPerformance(String Industry,String Ticker,String StartDate,String EndDate,String TickerReturn,String IndustryReturn) throws Exception{
        PreparedStatement pstmt;
        pstmt = writeConnect.prepareStatement(insertPerformance);
        pstmt.setString(1,Industry);
        pstmt.setString(2,Ticker);
        pstmt.setString(3,StartDate);
        pstmt.setString(4,EndDate);
        pstmt.setString(5,TickerReturn);
        pstmt.setString(6,IndustryReturn);
        pstmt.executeUpdate();
        pstmt.close();

   }


   static void createPerformanceTable() throws Exception{
        Statement stmt = null;
        stmt = writeConnect.createStatement();
        stmt.executeUpdate(dropPerformanceTable);
        stmt.executeUpdate(createPerformanceTable);
        stmt.close();

   }

   static ArrayList<String> distinctTickersInIndustry(String startDate,String endDate,String industry) throws Exception{
        PreparedStatement pstmt;
        ResultSet rs;
        ArrayList<String> distinctTickers = new ArrayList<String>();
        pstmt = readConnect.prepareStatement("SELECT distinct Ticker " +
                          " FROM (SELECT Ticker,count(distinct TransDate) as TradingDays " +
                            " FROM Company natural join PriceVolume " +
                                "WHERE Industry = ? " + "AND TransDate >= ? " +
                                    "AND TransDate <= ? " +
                                      "GROUP BY Ticker " +
                                        "HAVING TradingDays >= 150) as Tickers");

        pstmt.setString(1,industry);
        pstmt.setString(2,startDate);
        pstmt.setString(3,endDate);
        rs = pstmt.executeQuery();
        while(rs.next()){
            distinctTickers.add(rs.getString(1));
        }
        pstmt.close();
        return distinctTickers;
   }

   static void mapTickerToIntervals(HashMap<String,List<IntervalDataRange>> tickerToIntervals,List<TickerInfo> tickerData,int tradingDayIntervals,int distinctTickers){
   	    int tickerDataSize = tickerData.size();
	     String firstTickerName = tickerData.get(0).getTicker();
        tickerToIntervals.put(firstTickerName,new LinkedList<IntervalDataRange>());
        List<IntervalDataRange> currentList = tickerToIntervals.get(firstTickerName);

        for(int i = 0; i < tradingDayIntervals; i++) {
		        currentList.add(new IntervalDataRange(tickerData.get(INTERVAL_RANGE * i),tickerData.get(i * INTERVAL_RANGE + INTERVAL_RANGE - 1),i));
		    }

        String tickerName;
        int tickerDataIndex = (tradingDayIntervals * INTERVAL_RANGE) - 1;

        while(tickerData.get(tickerDataIndex).getTicker().compareTo(firstTickerName) == 0){
            tickerDataIndex++;
        }

        TickerInfo currentStartDate;
        TickerInfo currentEndDate;
        String intervalStartDate;
        String nextIntervalStartDate;
        for(int i = 1; i <= distinctTickers - 1; i++){
            tickerName = tickerData.get(tickerDataIndex).getTicker();
            tickerToIntervals.put(tickerName,new LinkedList<IntervalDataRange>());
            currentList = tickerToIntervals.get(tickerName);
            for(int j = 0; j < tradingDayIntervals - 1;j++){
                intervalStartDate = tickerToIntervals.get(firstTickerName).get(j).startDate;
                nextIntervalStartDate = tickerToIntervals.get(firstTickerName).get(j + 1).startDate;

                while(intervalStartDate.compareTo(tickerData.get(tickerDataIndex).getTransDate()) > 0){
                    tickerDataIndex++;
                }
                

                currentStartDate = tickerData.get(tickerDataIndex);

                while(nextIntervalStartDate.compareTo(tickerData.get(tickerDataIndex).getTransDate()) > 0){
                    tickerDataIndex++;
                }

                tickerDataIndex--;

                currentEndDate = tickerData.get(tickerDataIndex);
                currentList.add(new IntervalDataRange(currentStartDate,currentEndDate,j));
                tickerDataIndex++;
            }
            intervalStartDate = tickerToIntervals.get(firstTickerName).get(tradingDayIntervals - 1).startDate;
            String intervalEndDate = tickerToIntervals.get(firstTickerName).get(tradingDayIntervals - 1).endDate;

            while(intervalStartDate.compareTo(tickerData.get(tickerDataIndex).getTransDate()) > 0){
                    tickerDataIndex++;
            }

            currentStartDate = tickerData.get(tickerDataIndex);
            tickerDataIndex++;

            while(intervalEndDate.compareTo(tickerData.get(tickerDataIndex).getTransDate()) > 0){
               tickerDataIndex++;
            }

            if(intervalEndDate.compareTo(tickerData.get(tickerDataIndex).getTransDate()) < 0){
               tickerDataIndex--;
            }

            currentEndDate = tickerData.get(tickerDataIndex);
            currentList.add(new IntervalDataRange(currentStartDate,currentEndDate,tradingDayIntervals));
            while(tickerDataIndex < tickerDataSize && tickerData.get(tickerDataIndex).getTicker().compareTo(tickerName) == 0){
                tickerDataIndex++;
            }
       }

      }


    static int determineTradingDayIntervals (String startDate, String endDate,String Industry) throws Exception{
        PreparedStatement pstmt;
        ResultSet rs;
        pstmt = readConnect.prepareStatement("SELECT min(TradingDays) FROM (SELECT count(distinct TransDate) as TradingDays" +
                            " FROM Company natural join PriceVolume " +
                                "WHERE Industry = ? " + "AND TransDate >= ? " +
                                    "AND TransDate <= ? " +
                                      "GROUP BY Ticker " +
                                        "HAVING TradingDays >= 150" +
                                            " ORDER BY Ticker)" + " as StockTradingDays");
        pstmt.setString(1,Industry);
        pstmt.setString(2,startDate);
        pstmt.setString(3,endDate);
        rs = pstmt.executeQuery();
        int minTradingDays = 0;
        if(rs.next()){
            minTradingDays = rs.getInt(1);
        }
        pstmt.close();
        return minTradingDays/INTERVAL_RANGE;

   }


  static LinkedList<TickerInfo> applyStockSplits(String Industry,String startDate,String endDate,ArrayList<String> distinctTickers)throws SQLException{
				TickerInfo tickerInfo;
        LinkedList<TickerInfo> priceVolumeReverseOrder;
        LinkedList<TickerInfo> priceVolume = new LinkedList<TickerInfo>();
				double divisor = 1.0;
				double openingPrice	= 0;
				double closingPrice	= 0;
				PreparedStatement pstmt = null;
				ResultSet rs;

        for(int i = 0; i < distinctTickers.size(); i++) {
				    priceVolumeReverseOrder = new LinkedList<TickerInfo>();
            divisor = 1.0;
				    openingPrice = 0;
				    closingPrice = 0;

    			  pstmt =	readConnect.prepareStatement("SELECT Ticker,TransDate,OpenPrice,ClosePrice " +
                                                         "FROM PriceVolume " +
                                                         "WHERE Ticker = ? AND TransDate >= ? " +
                                                         "AND TransDate <= ? " +
                                                         "ORDER BY TransDate desc");
    			  pstmt.setString(1,distinctTickers.get(i));
            pstmt.setString(2,startDate);
            pstmt.setString(3,endDate);

    				rs = pstmt.executeQuery();
    				if(rs.next()){
    								openingPrice = rs.getDouble(3);
    								tickerInfo = new TickerInfo(rs.getString(1),rs.getString(2),rs.getDouble(3),
    								rs.getDouble(4));
    								priceVolumeReverseOrder.add(tickerInfo);

    							while(rs.next()){
    										closingPrice = rs.getDouble(4);
    										if(Math.abs((closingPrice/openingPrice)	- 1.5) < .15){
    											divisor	= divisor *	1.5;
    										}

    										else if	(Math.abs((closingPrice/openingPrice) -	2.0) < .2){
    											divisor	= divisor *	2.0;
    								     
    										} else if(Math.abs((closingPrice/openingPrice) - 3.0) <	.3)	{
    											divisor	= divisor *	3.0;
    										 
    										}
    										tickerInfo = new TickerInfo(rs.getString(1),rs.getString(2),rs.getDouble(3),
    								    rs.getDouble(4));
    										priceVolumeReverseOrder.add(tickerInfo);
    										openingPrice = rs.getDouble(3);
    							}
    				}
                    for(int j = priceVolumeReverseOrder.size() - 1; j >= 0; j--){
                        priceVolume.add(priceVolumeReverseOrder.get(j));
                    }
                }
                pstmt.close();
                return priceVolume;
	 }


     static String[] findDataRange(String [] dateRange,String industry) throws SQLException {

            PreparedStatement pstmt;
            ResultSet rs;

            pstmt = readConnect.prepareStatement("SELECT Ticker,min(TransDate),"
                        + "max(TransDate),count(distinct TransDate) as TradingDays" +
                            " FROM Company natural join PriceVolume " +
                                "WHERE Industry = ? GROUP BY Ticker " +
                                        "HAVING TradingDays >= 150" +
                                            " ORDER BY Ticker");
            pstmt.setString(1,industry);
            rs = pstmt.executeQuery();
                if(rs.next()) {
                    dateRange[0] = rs.getString(2);
                    dateRange[1] = rs.getString(3);
                    while(rs.next()){

                        if(rs.getString(2).compareTo(dateRange[0]) > 0){
                            dateRange[0] = rs.getString(2);
                        }

                        if(rs.getString(3).compareTo(dateRange[1]) < 0) {
                            dateRange[1]  = rs.getString(3);
                        }
                    }

                 }
            return dateRange;
       }

       static class IntervalDataRange {
            String startDate;
            TickerInfo firstDayOfInterval;
            TickerInfo lastDayOfInterval;
            String  endDate;
            int intervalNumber;

            IntervalDataRange(TickerInfo startDate, TickerInfo  endDate, int intervalNumber) {
                this.firstDayOfInterval = startDate;
                this.lastDayOfInterval = endDate;
                this.startDate = startDate.getTransDate();
                this.endDate = endDate.getTransDate();
                this.intervalNumber = intervalNumber;
            }

       }


 }
