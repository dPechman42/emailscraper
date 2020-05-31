import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Scrapper
{

    private final  Set<String> linksVisited;
    private final  Set<String> emailsUploaded;
    private final Set<String> synchedEmail;
    private LinkedBlockingQueue<String> synchedLink;
    private ExecutorService exe;
    private int lastUpLoadCount;
    private int numSites;
    private Pattern pattern;



    public Scrapper() throws InterruptedException
    {
        this.linksVisited = Collections.synchronizedSet(new HashSet<>());
        this.emailsUploaded = Collections.synchronizedSet(new HashSet<>());
        this.synchedEmail = Collections.synchronizedSet(new HashSet<>());
        this.synchedLink = new LinkedBlockingQueue<>();
        this.exe = Executors.newFixedThreadPool(50);
        this.pattern = Pattern.compile("\\b([^\\s][a-zA-Z0-9_\\-.]+)@([a-zA-Z0-9_\\-.]+)\\.([a-zA-Z]{2,})\\b");

        this.lastUpLoadCount = 0;
        this.numSites = 0;

        synchedLink.add("https://www.touro.edu/");
        linksVisited.add("https://www.touro.edu/");

        int MAX_EMAIL = 10000;
        String link;

        while (synchedEmail.size() + lastUpLoadCount < MAX_EMAIL)
        {
            if (!synchedLink.isEmpty()) {
                link = synchedLink.poll();
                numSites++;
                if (batchForDb()) {
                    Thread.sleep(500);
                    dbUpload();
                }
                if (numSites % 100_000 <= 200){
                    linksVisited.clear();
                }
                exe.execute(new EmailScraper(link));
            }
            //Thread.sleep(100);
        }
        exe.shutdown();

        try{
            if (!exe.awaitTermination(60, TimeUnit.SECONDS)){
                exe.shutdownNow();
            }
        }catch (InterruptedException ie){
            exe.shutdownNow();
            Thread.currentThread().interrupt();
        }

        while (!exe.isTerminated())
        {
            if (batchForDb()){
                dbUpload();
            }
            Thread.sleep(500);
        }
        dbUpload();
        System.out.println("uploadedEmails: " + lastUpLoadCount);
    }


    public static void main(String[] args) throws InterruptedException
    {
        Scrapper web = new Scrapper();
    }

    private class EmailScraper implements Runnable
    {

        private String hyperLink;

        EmailScraper(String hyperLink)
        {
            this.hyperLink = hyperLink;
        }


        @Override
        public void run()
        {

            try {
                Document site = Jsoup.connect(hyperLink).ignoreHttpErrors(true).ignoreContentType(true).get();

                Elements link = site.select("a[href]");
                String l;
                for (Element e : link){
                    l = e.attr("abs:href");
                    if (l.contains("facebook") || l.contains("linkedin") || l.contains("/watch") ||
                            l.contains("amazon") || l.contains("vimeo") ||
                            l.contains("mailto") || l.contains("apple") || l.contains("instagram") ||
                            l.contains(".pdf") || l.contains(".jpg") || l.contains("png")){
                        linksVisited.add(l);
                    }else {
                        if (linksVisited.add(l)) {
                            synchedLink.add(l);
                        }
                    }
                }
                Elements em = site.getElementsMatchingOwnText("\\b[a-zA-Z0-9_\\.+\\-]+@[a-zA-Z0-9\\.\\-]+\\.[a-zA-Z]{2,}\\b");
                for (Element e : em){
                    Matcher match = pattern.matcher(e.text().toLowerCase().trim());
                    if (match.find()) {
                        synchedEmail.add(match.group());
                    }
                }


            } catch (IOException e) {
                e.printStackTrace();
            }


            synchedEmail.removeAll(emailsUploaded);
            System.out.println("LinkSize: " + numSites + " : EmailSize: " + synchedEmail.size() +
                    " : uploadedEmails: " + lastUpLoadCount);
            System.out.println(exe);
            System.out.println(Thread.activeCount());
            try {
                Thread.sleep(4000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    private boolean batchForDb()
    {
        final int BATCH_SIZE = 500;
        return (synchedEmail.size() + emailsUploaded.size()) - lastUpLoadCount > BATCH_SIZE;
    }

    private void dbUpload()
    {
        String url = "database-1.cbjmpwcdjfmq.us-east-1.rds.amazonaws.com:1433";
        String connectionUrl =
                String.format("jdbc:sqlserver://%s;databaseName=pechman;user=admin;password=mco368Touro", url);

        Set<String> localCopy = new HashSet<>(synchedEmail);
        try (Connection con = DriverManager.getConnection(connectionUrl);
             Statement stmt = con.createStatement()) {
            for (String email : localCopy) {
                String se = email.replace("'", "''");
                String insertQuery = String.format("INSERT INTO emails (Emails) VALUES ('%s')", se);
                stmt.executeUpdate(insertQuery);
                emailsUploaded.add(email);
                lastUpLoadCount++;
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
