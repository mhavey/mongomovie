package org.jude.bigdata.recroom.movies.etl.test;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jude.bigdata.recroom.movies.etl.ETLException;
import org.jude.bigdata.recroom.movies.etl.parser.ActorsFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.ActressesFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.AkaNamesFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.AkaTitlesFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.AlternateVersionsFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.BiographiesFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.BusinessFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.CertificatesFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.CinematographersFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.ColorInfoFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.ComposersFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.CostumeDesignersFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.CountriesFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.CrazyCreditsFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.DirectorsFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.DistributorsFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.EditorsFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.GenresFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.GoofsFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.ImdbLineParser;
import org.jude.bigdata.recroom.movies.etl.parser.KeywordsFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.LanguageFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.LiteratureFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.LocationsFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.MiscellaneousCompaniesFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.MiscellaneousFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.MoviesFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.MpaaRatingsReasonsFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.PlotFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.ProducersFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.ProductionCompaniesFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.ProductionDesignersFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.QuotesFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.RatingsFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.RunningTimesFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.SoundMixFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.SoundtracksFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.SpecialEffectsCompaniesFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.TaglinesFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.TriviaFileParser;
import org.jude.bigdata.recroom.movies.etl.parser.WritersFileParser;

import com.mongodb.BasicDBObject;

public class TestLineParser {

	static void runParser(ImdbLineParser parser) throws ETLException {
		try {
			parser.openReader();
			int numIter = 0;
			int numFails = 0;
			long startTime = System.currentTimeMillis();
			int printInterval = 50000;
			while (true) {
				try {
					BasicDBObject t = parser.next();
					if (t == null) {
						break;
					}
					if ((numIter % printInterval) == 0) {
						System.out.println(t);
					}
					if (t.toString().contains("Annie Hall")) {
						System.out.println(t);
					}
				} catch (ETLException x) {
					numFails++;
					System.out.println(x);
					x.printStackTrace();
				}
				numIter++;
			}
			long endTime = System.currentTimeMillis();
			System.out.println(parser.getClass().getName() + "," + numIter
					+ "," + numFails + "," + parser.getLastLineNumber() + ","
					+ (endTime - startTime));
		} finally {
			parser.closeReader();
		}
	}

	/**
	 * @param args
	 * @throws ETLException
	 */
	public static void main(String[] args) throws ETLException {
		BasicConfigurator.configure();
		Logger.getRootLogger().setLevel(Level.INFO);

		String path = "C:\\mike\\tibco\\2\\self_train\\packt\\release\\imdb";
		/*
		runParser(new MoviesFileParser(path));
		runParser(new AkaTitlesFileParser(path));
		runParser(new CountriesFileParser(path));
		runParser(new LanguageFileParser(path));
		runParser(new GenresFileParser(path));
		runParser(new KeywordsFileParser(path));
		runParser(new LocationsFileParser(path));
		runParser(new RunningTimesFileParser(path));

		runParser(new RatingsFileParser(path));
		*/
		//runParser(new CertificatesFileParser(path));
		//runParser(new MpaaRatingsReasonsFileParser(path));

		/*
		runParser(new ColorInfoFileParser(path));
		*/
		//runParser(new SoundMixFileParser(path));
		runParser(new BusinessFileParser(path));
		/*

		runParser(new CrazyCreditsFileParser(path));
		runParser(new LiteratureFileParser(path));
		runParser(new SoundtracksFileParser(path));
		runParser(new TriviaFileParser(path));
		runParser(new TaglinesFileParser(path));
		runParser(new QuotesFileParser(path));
		runParser(new PlotFileParser(path));
		runParser(new GoofsFileParser(path));
		runParser(new AlternateVersionsFileParser(path));
*/
		/*
		 * 
		 * runParser(new BiographiesFileParser(path)); runParser(new
		 * AkaNamesFileParser(path));
		 * 
		 * runParser(new ActressesFileParser(path)); runParser(new
		 * ActorsFileParser(path)); runParser(new
		 * MiscellaneousFileParser(path)); runParser(new
		 * CinematographersFileParser(path)); runParser(new
		 * ComposersFileParser(path)); runParser(new
		 * CostumeDesignersFileParser(path)); runParser(new
		 * DirectorsFileParser(path)); runParser(new EditorsFileParser(path));
		 * runParser(new ProducersFileParser(path)); runParser(new
		 * ProductionDesignersFileParser(path)); runParser(new
		 * WritersFileParser(path));
		 * 
		 * runParser(new DistributorsFileParser(path)); runParser(new
		 * MiscellaneousCompaniesFileParser(path)); runParser(new
		 * ProductionCompaniesFileParser(path)); runParser(new
		 * SpecialEffectsCompaniesFileParser(path));
		 */
	}
}
