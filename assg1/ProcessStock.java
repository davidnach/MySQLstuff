import java.util.Properties;
import java.util.Scanner;
import java.io.FileInputStream;
import java.sql.*;
import java.util.Scanner;
import java.util.Date;
import java.util.Deque;
import java.util.ArrayDeque;

public class ProcessStock {
             static Connection connect = null;
             
             public static void main(String[] args) throws Exception {
                Scanner sc = new Scanner(System.in);
                String[] userArgs;
                String startDate = null;
                String endDate = null;
                boolean tickerFound = false;
                String userInput;
                String ticker;
                String paramsFile = "ConnectionParameters.txt";
                Properties connectProps = new Properties();
                connectProps.load(new FileInputStream(paramsFile));
                
                try {
                    Class.forName("com.mysql.jdbc.Driver");
                    String dburl = connectProps.getProperty("dburl");
                    String username = connectProps.getProperty("user");
                    connect = DriverManager.getConnection(dburl, connectProps);
                    System.out.printf("Database connection %s %s established.%n",dburl,username);
                }catch(Exception e){
                    e.printStackTrace();
                }
             
                while(true){
                    System.out.println("Enter ticker symbol [start/end dates]: ");
                    userInput = sc.nextLine();
                    userArgs = userInput.split(" ");
                    if(userArgs.length < 1){
                        System.exit(0);//user termination
                    }
                    if(userArgs.length == 1 || userArgs.length == 3){
                        ticker = userArgs[0];
                        if(userArgs.length == 3){
                            startDate = userArgs[1];
                            endDate = userArgs[2];
                        }
                        tickerFound = getCompanyName(userArgs[0]);
                        adjustStockPrices(ticker,startDate,endDate);
                    }
                 }
             }
             
             static boolean getCompanyName(String ticker) throws SQLException {
                boolean tickerFound = false;
                PreparedStatement pstmt = connect.prepareStatement("select Name " + " from Company " + " where Ticker = ? ");                         
                pstmt.setString(1, ticker);
                ResultSet rs = pstmt.executeQuery();
                if(rs.next()){
                    System.out.println(rs.getString(1));
                    tickerFound = true;
                } else {
                    System.out.printf("Ticker %s not found",ticker);
                }
                pstmt.close();
                return tickerFound;
             }
             
             static void adjustStockPrices(String ticker,String startDate,String endDate)throws SQLException{
               
                Deque priceVolume = new ArrayDeque();
                String a;
                TickerInfo tickerInfo;
                int tradingDays = 0;
                int splitCount = 0;
                double divisor = 1.0;
                double openingPrice = 0;
                double closingPrice = 0;
                boolean dateProvided = false;
                if(startDate != null && endDate != null){
                    dateProvided = true;
                }
                PreparedStatement pstmt;
                if(dateProvided){
                    pstmt = connect.prepareStatement("select TransDate,OpenPrice,HighPrice,LowPrice,ClosePrice " + " from PriceVolume " + " where Ticker = ? " + 
                    "AND TransDate >= ? AND TransDate <= ?" + " order by TransDate DESC");
                    pstmt.setString(1,ticker);
                    pstmt.setString(2,startDate);
                    pstmt.setString(3,endDate);
                    
                }else {
                    pstmt = connect.prepareStatement("select TransDate,OpenPrice,HighPrice,LowPrice,ClosePrice " + " from PriceVolume " + " where Ticker = ? order by TransDate DESC");
                    pstmt.setString(1,ticker);
                ResultSet rs = pstmt.executeQuery();
                if(rs.next()){
                    while(rs.next()){
                        rs.previous();
                            openingPrice = rs.getDouble(2);
                            a = rs.getString(1);
                            if(tradingDays == 0){
                                tickerInfo = new TickerInfo(ticker,rs.getString(1),rs.getDouble(2)/divisor,
                                rs.getDouble(3)/divisor,rs.getDouble(4)/divisor,rs.getDouble(5)/divisor);
                                priceVolume.add(tickerInfo);
                                tradingDays++;
                            }
                            if(rs.next()){
                                        tradingDays++;
                                        closingPrice = rs.getDouble(5);
                                        if(Math.abs((closingPrice/openingPrice) - 1.5) < .15){
                                            divisor = divisor * 1.5;
                                            splitCount++;
                                            System.out.println("3:2 split on " + rs.getString(1) + " " + closingPrice + "--> " + openingPrice);
                                        }else if(Math.abs((closingPrice/openingPrice) - 2.0) < .2){
                                            divisor = divisor * 2;
                                            splitCount++;
                                            System.out.println("2:1 split on " + rs.getString(1) + " " + closingPrice + "--> " + openingPrice);
                                        }else if(Math.abs((closingPrice/openingPrice) - 3.0) < .3) {
                                            divisor = divisor * 3;
                                            splitCount++;
                                            System.out.println("3:1 split on " + rs.getString(1) + " " + closingPrice + "--> " + openingPrice);
                                        }
                                        tickerInfo = new TickerInfo(ticker,rs.getString(1),rs.getDouble(2)/divisor,
                                        rs.getDouble(3)/divisor,rs.getDouble(4)/divisor,rs.getDouble(5)/divisor);
                                        priceVolume.add(tickerInfo);
                            }
                     }
                }
              }
               System.out.println(splitCount + " splits in " + tradingDays +  " trading days");
               pstmt.close();
            }
}