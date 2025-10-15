import java.sql.*;

public class sqlAssignment3RDS {
    private Connection con;
    private String url = "sarthakdb.cqfaysk0aij9.us-east-1.rds.amazonaws.com";
    private String uid = "sarthak";
    private String pw = "Sarthak_123";
    private String dbName = "sarthakdb";

    public void connect() throws SQLException, ClassNotFoundException {
        // Load SQL Server JDBC driver
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

        // Connection string for SQL Server on AWS RDS
        String jdbcUrl = "jdbc:sqlserver://" + url + ":1433;" +
                "databaseName=" + dbName + ";" +
                "user=" + uid + ";" +
                "password=" + pw + ";" +
                "encrypt=true;trustServerCertificate=true;";
        System.out.println("Connecting to database");
        con = DriverManager.getConnection(jdbcUrl);
        System.out.println("Connection Successful!");
    }

    public void drop() throws SQLException {
        Statement stmt = con.createStatement();
        stmt.executeUpdate("DROP TABLE IF EXISTS stockprice;");
        stmt.executeUpdate("DROP TABLE IF EXISTS company;");
        System.out.println("Tables dropped successfully!");
    }

    public void create() throws SQLException {
        Statement stmt = con.createStatement();
        String createCompany = "CREATE TABLE company (" +
                "id INT PRIMARY KEY, " +
                "name VARCHAR(50), " +

                "ticker CHAR(10), " +
                "annualRevenue DECIMAL(15,2), " +
                "numEmployees INT);";
        String createStockPrice = "CREATE TABLE stockprice (" +
                "companyId INT, " +
                "priceDate DATE, " +
                "openPrice DECIMAL(10,2), " +
                "highPrice DECIMAL(10,2), " +
                "lowPrice DECIMAL(10,2), " +
                "closePrice DECIMAL(10,2), " +
                "volume INT, " +
                "PRIMARY KEY(companyId, priceDate), " +
                "FOREIGN KEY(companyId) REFERENCES company(id));";
        stmt.executeUpdate(createCompany);
        stmt.executeUpdate(createStockPrice);
        System.out.println("Tables created successfully!");
    }

    public void insert() throws SQLException {
        Statement stmt = con.createStatement();
        // --- Insert data into company table ---
        stmt.executeUpdate("INSERT INTO company VALUES " +
                "(1, 'Apple', 'AAPL', 387540000000.00, 154000)," +

                "(2, 'GameStop', 'GME', 611000000.00, 12000)," +
                "(3, 'Handy Repair', NULL, 2000000.00, 50)," +
                "(4, 'Microsoft', 'MSFT', 198270000000.00, 221000)," +
                "(5, 'StartUp', NULL, 50000.00, 3);");
        System.out.println("Company data inserted successfully!");
        // --- Insert data into stockprice table ---
        String insertStock = "INSERT INTO stockprice VALUES " +
                "(1, '2022-08-15', 171.52, 173.39, 171.35, 173.19, 54091700)," +
                "(1, '2022-08-16', 172.78, 173.71, 171.66, 173.03, 56377100)," +
                "(1, '2022-08-17', 172.77, 176.15, 172.57, 174.55, 79542000)," +
                "(1, '2022-08-18', 173.75, 174.90, 173.12, 174.15, 62290100)," +
                "(1, '2022-08-19', 173.03, 173.74, 171.31, 171.52, 70211500)," +
                "(1, '2022-08-22', 169.69, 169.86, 167.14, 167.57, 69026800)," +
                "(1, '2022-08-23', 167.08, 168.71, 166.65, 167.23, 54147100)," +
                "(1, '2022-08-24', 167.32, 168.11, 166.25, 167.53, 53841500)," +
                "(1, '2022-08-25', 168.78, 170.14, 168.35, 170.03, 51218200)," +
                "(1, '2022-08-26', 170.57, 171.05, 163.56, 163.62, 78823500)," +
                "(1, '2022-08-29', 161.15, 162.90, 159.82, 161.38, 73314000)," +
                "(1, '2022-08-30', 162.13, 162.56, 157.72, 158.91, 77906200)," +
                // GameStop (companyId = 2)
                "(2, '2022-08-15', 39.75, 40.39, 38.81, 39.68, 5243100)," +
                "(2, '2022-08-16', 39.17, 45.53, 38.60, 42.19, 23602800)," +
                "(2, '2022-08-17', 42.18, 44.36, 40.41, 40.52, 9766400)," +
                "(2, '2022-08-18', 39.27, 40.07, 37.34, 37.93, 8145400)," +
                "(2, '2022-08-19', 35.18, 37.19, 34.67, 36.49, 9525600)," +
                "(2, '2022-08-22', 34.31, 36.20, 34.20, 34.50, 5798600)," +
                "(2, '2022-08-23', 34.70, 34.99, 33.45, 33.53, 4836300)," +
                "(2, '2022-08-24', 34.00, 34.94, 32.44, 32.50, 5620300)," +
                "(2, '2022-08-25', 32.84, 32.89, 31.50, 31.96, 4726300)," +
                "(2, '2022-08-26', 31.50, 32.38, 30.63, 30.94, 4289500)," +
                "(2, '2022-08-29', 30.48, 32.75, 30.38, 31.55, 4292700)," +
                "(2, '2022-08-30', 31.62, 31.87, 29.42, 29.84, 5060200)," +
                // Microsoft (companyId = 4)
                "(4, '2022-08-15', 291.00, 294.18, 290.11, 293.47, 18085700)," +
                "(4, '2022-08-16', 291.99, 294.04, 290.42, 292.71, 18102900)," +
                "(4, '2022-08-17', 289.74, 293.35, 289.47, 291.32, 18253400)," +
                "(4, '2022-08-18', 290.19, 291.91, 289.08, 290.17, 17186200)," +
                "(4, '2022-08-19', 288.90, 289.25, 285.56, 286.15, 20557200)," +
                "(4, '2022-08-22', 282.08, 282.46, 277.22, 277.75, 25061100)," +
                "(4, '2022-08-23', 276.44, 278.86, 275.40, 276.44, 17527400)," +
                "(4, '2022-08-24', 275.41, 277.23, 275.11, 275.79, 18137000)," +
                "(4, '2022-08-25', 277.33, 279.02, 274.52, 278.85, 16583400)," +
                "(4, '2022-08-26', 279.08, 280.34, 267.98, 268.09, 27532500)," +
                "(4, '2022-08-29', 265.85, 267.40, 263.85, 265.23, 20338500)," +
                "(4, '2022-08-30', 266.67, 267.05, 260.66, 262.97, 22767100);";
        stmt.executeUpdate(insertStock);

        System.out.println("Stock price data inserted successfully!");
    }

    public void delete() throws SQLException {
        Statement stmt = con.createStatement();
        String sql = "DELETE FROM stockprice " +
                "WHERE priceDate < '2022-08-20' " +
                "OR companyId IN (SELECT id FROM company WHERE name = 'GameStop');";
        int rows = stmt.executeUpdate(sql);
        System.out.println("Deleted " + rows + " rows.");
    }

    public void close() throws SQLException {
        if (con != null) {
            con.close();
            System.out.println("Connection Closed"); // Added confirmation
        }
    }

    public ResultSet queryOne() throws SQLException {
        System.out.println("-------------------------------------------------");
        System.out.println("Query1 Results: ");
        System.out.println("-------------------------------------------------");
        Statement stmt = con.createStatement();

        String sql = """
                select name, annualRevenue as revenue, numEmployees as employees
                from company
                where annualRevenue < 1000000
                or numEmployees > 10000
                order by name asc;
                """;
        ResultSet rs = stmt.executeQuery(sql);
        // Print metadata (schema info)
        System.out.println("=== QueryOne Metadata ===");
        System.out.println(resultSetMetaDataToString(rs.getMetaData()));
        // Print actual data
        System.out.println("=== QueryOne Results ===");
        System.out.println(resultSetToString(rs, 100));
        return rs;
    }

    public ResultSet queryTwo() throws SQLException {
        System.out.println("-------------------------------------------------");
        System.out.println("Query2 Results: ");
        System.out.println("-------------------------------------------------");
        Statement stmt = con.createStatement();
        String sql = """
                select comp.name as company_name

                ,ticker
                ,min(sp.lowPrice) as lowest_price
                ,max(sp.highPrice) as highest_price
                ,avg(closePrice) as closing_price
                ,avg(volume) as average_volume
                from company comp
                join stockprice sp
                on comp.id = sp.companyId
                where priceDate between '2022-08-22' and '2022-08-26'
                group by name, ticker
                order by average_volume desc;
                """;
        ResultSet rs = stmt.executeQuery(sql);
        // Print metadata (schema info)
        System.out.println("=== QueryTwo Metadata ===");
        System.out.println(resultSetMetaDataToString(rs.getMetaData()));
        // Print actual data
        System.out.println("=== QueryTwo Results ===");
        System.out.println(resultSetToString(rs, 100));
        return rs;
    }

    public ResultSet queryThree() throws SQLException {
        System.out.println("-------------------------------------------------");

        System.out.println("Query3 Results: ");
        System.out.println("-------------------------------------------------");
        Statement stmt = con.createStatement();
        String sql = """
                SELECT
                c.name,
                c.ticker,
                p.closePrice AS closing_price_aug30
                FROM company c
                LEFT JOIN stockprice p
                ON c.id = p.companyId AND p.priceDate = '2022-08-30'
                LEFT JOIN (
                SELECT
                companyId,
                AVG(closePrice) AS avg_week_close
                FROM stockprice
                WHERE priceDate BETWEEN '2022-08-15' AND '2022-08-19'
                GROUP BY companyId
                ) w ON c.id = w.companyId
                WHERE
                (p.closePrice <= 0.1 * w.avg_week_close OR c.ticker IS NULL)
                ORDER BY c.name ASC;
                """;
        ;
        ResultSet rs = stmt.executeQuery(sql);
        // Print metadata (schema info)
        System.out.println("=== QueryThree Metadata ===");

        System.out.println(resultSetMetaDataToString(rs.getMetaData()));
        // Print actual data
        System.out.println("=== QueryThree Results ===");
        System.out.println(resultSetToString(rs, 100));
        return rs;
    }

    public static String resultSetToString(ResultSet rst, int maxrows) throws SQLException {
        StringBuffer buf = new StringBuffer(5000);
        int rowCount = 0;
        if (rst == null)
            return "ERROR: No ResultSet";
        ResultSetMetaData meta = rst.getMetaData();
        buf.append("Total columns: " + meta.getColumnCount());
        buf.append('\n');
        // Print Column Names
        if (meta.getColumnCount() > 0)
            buf.append(meta.getColumnName(1));
        for (int j = 2; j <= meta.getColumnCount(); j++)
            buf.append(", " + meta.getColumnName(j));
        buf.append('\n');
        // Print Rows
        while (rst.next()) {

            if (rowCount < maxrows) {
                // Column indices are 1-based in JDBC, so start from 1
                for (int j = 1; j <= meta.getColumnCount(); j++) {
                    Object obj = rst.getObject(j); // Use j, not j+1. Previous code used 0-based index which is
                                                   // incorrect.
                    buf.append(obj);
                    if (j != meta.getColumnCount())
                        buf.append(", ");
                }
                buf.append('\n');
            }
            rowCount++;
        }
        buf.append("Total results: " + rowCount);
        return buf.toString();
    }

    public static String resultSetMetaDataToString(ResultSetMetaData meta) throws SQLException {
        StringBuffer buf = new StringBuffer(5000);
        if (meta.getColumnCount() > 0) {
            // Print details for the first column
            buf.append(meta.getColumnName(1) + " ("
                    + meta.getColumnLabel(1) + ", "
                    + meta.getColumnType(1) + "-"
                    + meta.getColumnTypeName(1) + ", "

                    + meta.getColumnDisplaySize(1) + ","
                    + meta.getPrecision(1) + ", "
                    + meta.getScale(1) + ")");
            // Print details for subsequent columns
            for (int j = 2; j <= meta.getColumnCount(); j++) {
                buf.append(", " + meta.getColumnName(j) + " ("
                        + meta.getColumnLabel(j) + ","
                        + meta.getColumnType(j) + "-"
                        + meta.getColumnTypeName(j) + ", "
                        + meta.getColumnDisplaySize(j) + ","
                        + meta.getPrecision(j) + ", "
                        + meta.getScale(j) + ")");
            }
        }
        return buf.toString();
    }

    public static void main(String[] args) {
        sqlAssignment3RDS q = new sqlAssignment3RDS();
        try {
            q.connect();
            q.drop();
            q.create();
            q.insert();
            q.delete();

            q.queryOne();
            q.queryTwo();
            q.queryThree();
            q.close();
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}