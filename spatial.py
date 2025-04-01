import pandas as pd
import re
from collections import defaultdict
import geonamescache
from fuzzywuzzy import process
import nltk
from nltk import word_tokenize, pos_tag, ne_chunk
from nltk.tree import Tree
from tqdm import tqdm

# download necessary NLTK data files
nltk.download('punkt')
nltk.download('punkt_tab')
nltk.download('averaged_perceptron_tagger_eng')
nltk.download('maxent_ne_chunker_tab')
nltk.download('words')

class LocationExtractor:
    def __init__(self, countries=None):
        """
        Initialize the LocationExtractor with a dictionary of countries.
        
        Args:
            countries (dict): Dictionary mapping country names to ISO country codes
        """
        # Default countries if none provided
        self.country_codes = countries or {
            'Myanmar': 'MM',
            'Bangladesh': 'BD',
            'Thailand': 'TH',
            'India': 'IN',
            'China': 'CN',
            'Laos': 'LA',
            'Vietnam': 'VN',
            'Cambodia': 'KH',
            'United States': 'US',
        }
        
        self.gc = geonamescache.GeonamesCache()
        self.location_hierarchy = defaultdict(list)
        self.location_to_country = {}
        
        # Build the location hierarchy and mappings
        self._build_location_hierarchy()
        
    def _build_location_hierarchy(self):
        """Build the location hierarchy from GeonamesCache data."""
        cities = self._get_cities_by_country_code()
        
        for city in cities:
            country_code = city['countrycode']
            city_name = city['name']
            # Find the country name from the country_codes dictionary
            country_name = next((key for key, value in self.country_codes.items() 
                                if value == country_code), None)
            if country_name:
                if city_name not in self.location_hierarchy[country_name]:
                    self.location_hierarchy[country_name].append(city_name)
        
        # Flatten the hierarchy for lookup
        for country, cities in self.location_hierarchy.items():
            for city in cities:
                self.location_to_country[city] = country
    
    def _get_cities_by_country_code(self):
        """Get cities by country codes from GeonamesCache."""
        cities = self.gc.get_cities()
        relevant_cities = []
        # get cities by country code
        for city in cities.values():
            if city['countrycode'] in self.country_codes.values():
                relevant_cities.append(city)
        return relevant_cities
    
    def _fuzzy_match_location(self, name, choices, threshold=99):
        """Find the best match for a location name using fuzzy matching."""
        match, score = process.extractOne(name, choices)
        return match if score >= threshold else None
    
    def extract_locations(self, text):        
        """Extract locations from text using NLTK's named entity recognition."""
        if not text:
            return []
        # strip out #
        text = re.sub(r'#\w+', '', text)
        
        # Tokenize, tag, and chunk
        tokens = word_tokenize(text)
        tagged_tokens = pos_tag(tokens)
        named_entities = ne_chunk(tagged_tokens)
        
        # Extract locations (GPE and LOC entities)
        locations = []
        for subtree in named_entities:
            if type(subtree) == Tree:
                if subtree.label() == 'GPE' or subtree.label() == 'LOC':
                    location = " ".join([token for token, pos in subtree.leaves()])
                    locations.append(location)
        
        # Find potential locations with regex
        # potential_locations = re.findall(r'\b([A-Z][a-z]+(?:\s[A-Z][a-z]+)*)\b', text)
        potential_locations = list(self.location_hierarchy.keys()) + [
            city for cities in self.location_hierarchy.values() for city in cities
        ]
        
        # remove locations which aren't in potential locations
        locations = [loc.lower() for loc in locations if loc in potential_locations]
        # remove empty strings
        locations = [loc for loc in locations if loc]
        # remove duplicates
        locations = list(set(locations))
        # for potential_loc in potential_locations:
        #     matched_loc = self._fuzzy_match_location(potential_loc, all_locations)
        #     if matched_loc and matched_loc not in locations:
        #         locations.append(matched_loc)

        return locations
    
    def organize_locations(self, locations):
        """Organize locations into a hierarchical structure (city, country)."""
        result = defaultdict(set)
        countries = set()
        cities = {}
        
        for loc in locations:
            if loc in self.location_hierarchy:
                # It's a country
                countries.add(loc)
            elif loc in self.location_to_country:
                # It's a city
                country = self.location_to_country[loc]
                cities[loc] = country
                countries.add(country)
        
        # Process cities first (more specific)
        for city, country in cities.items():
            result[country].add(city)
        
        # Add countries without cities
        for country in countries:
            if not result[country]:
                result[country] = set()
        
        return dict(result)
    
    def format_location(self, organized_locs):
        """Format the organized locations into a readable string."""
        if not organized_locs:
            return "Unknown location"
        
        result = []
        for country, cities in organized_locs.items():
            if cities:
                # Format as "City1, City2, Country"
                city_list = ", ".join(sorted(cities))
                result.append(f"{city_list}, {country}")
            else:
                # Just country
                result.append(country)
        
        return " | ".join(result)
    
    def process_posts(self, posts: pd.DataFrame) -> pd.DataFrame:
        """Process all posts and associate them with locations."""
        # remove rows with null values in 'translated_text' column
        cleaned_posts = posts.dropna(subset=['translated_text'])
        # Apply location extraction with progress bar
        tqdm.pandas(desc="Extracting locations")
        locations = cleaned_posts["translated_text"].progress_apply(self.extract_locations)
        return locations
        organized_locs = self.organize_locations(locations)
        cleaned_posts['locations'] = organized_locs
        cleaned_posts['formatted_location'] = cleaned_posts['locations'].apply(self.format_location)
        # join cleaned and original posts
        return cleaned_posts
