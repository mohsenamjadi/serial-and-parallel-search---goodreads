#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <chrono>
#include <bits/stdc++.h>
#include <thread>
#include <mutex>

using namespace std;

mutex mtx;

struct bookRate
{
	std::string id;
	std::string rating;
	std::string number_of_likes;	
};

struct Book
{
	std::string bookId;
	std::string bookTitle;
	std::string genre1;
	std::string genre2;
	std::string pages;
	std::string authorName;
	std::string authorAverageRating;	
};

struct bookRateFull
{
	std::string id;
	std::string rating;
	std::string number_of_likes;
	std::string authorAverageRating;	
};

struct bookRateForFormula
{
	std::string id;
	std::string numerator;
	std::string total_likes;
	std::string authorAverageRating;	
};



std::vector<Book> Search_in_booksFile(std::string&);
std::vector<bookRate> search_in_reviewsFile(std::vector<Book>&);
std::vector<bookRateFull> Complete_Rank(std::vector<Book>&, std::vector<bookRate>&);
std::string calculate_ratings(std::vector<bookRateFull>&);

int main(int argc, char const *argv[]) {

	auto start = std::chrono::high_resolution_clock::now(); 
   
    std::ios_base::sync_with_stdio(false); 

	std::string search_term = argv[1];
	std::vector<Book> bookData;
	std::vector<bookRate> bookRateData;
	std::vector<bookRateFull> bookRateFullData;

	bookData = Search_in_booksFile(search_term);

	bookRateData = search_in_reviewsFile(bookData);

	bookRateFullData = Complete_Rank(bookData, bookRateData);

	std::string max_rating_id = calculate_ratings(bookRateFullData);

	for (int i = 0; i < bookData.size(); ++i)
	{
		if(bookData[i].bookId == max_rating_id)
		{
			std::cout << "id :" << bookData[i].bookId << std::endl;
	    	std::cout << "Title :" << bookData[i].bookTitle << std::endl;
	    	std::cout << "Genres :" << bookData[i].genre1 << " , " << bookData[i].genre2 << std::endl;
	    	std::cout << "Number of Pages :" << bookData[i].pages << std::endl;
	    	std::cout << "Author :" << bookData[i].authorName << std::endl;
	    	std::cout << "Average Rating :" << bookData[i].authorAverageRating << std::endl;
		}
	}

	auto end = std::chrono::high_resolution_clock::now(); 

	double time_taken = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count(); 
  
    time_taken *= 1e-9; 
  
    std::cout << "Time taken by program is : " << std::fixed  
         << time_taken << std::setprecision(9); 
    std::cout << " sec" << std::endl; 

    
    return 0;

}

std::string calculate_ratings(std::vector<bookRateFull> &bookRateFullData)
{
	std::vector<bookRateForFormula> bookRateForFormula;
	for (int i = 0; i<bookRateFullData.size(); i++) 
	{ 
		//std::cout << bookRateFullData[i].id << ", " << bookRateFullData[i].rating 
		//	<< ", " << bookRateFullData[i].number_of_likes << ", " << bookRateFullData[i].authorAverageRating << std::endl; 
		bool flag = false;
		for(int j = 0; j < bookRateForFormula.size(); j++){
			if(bookRateFullData[i].id == bookRateForFormula[j].id){
				float temp = stof(bookRateFullData[i].number_of_likes) * stof(bookRateFullData[i].rating);
				//std::cout << temp << std::endl;
				bookRateForFormula[j].numerator = std::to_string( stof(bookRateForFormula[j].numerator) + temp );
				//std::cout << bookRateForFormula[j].numerator << std::endl;
				bookRateForFormula[j].total_likes = std::to_string( stof(bookRateFullData[i].number_of_likes) + stof(bookRateForFormula[j].total_likes) );
				flag = true;
			}
		}
		if(!flag){
			bookRateForFormula.push_back({bookRateFullData[i].id, std::to_string( stof(bookRateFullData[i].number_of_likes) * stof(bookRateFullData[i].rating) ), bookRateFullData[i].number_of_likes, bookRateFullData[i].authorAverageRating});
		}
	}

	/*for(int i = 0; i < bookRateForFormula.size(); i++){
		std::cout << bookRateForFormula[i].id << ", " << bookRateForFormula[i].numerator 
			<< ", " << bookRateForFormula[i].total_likes << ", " << bookRateForFormula[i].authorAverageRating << std::endl;
	}*/

	float max_rating_index = 0;
	float max_rating = 0;
	for(int i = 0; i < bookRateForFormula.size(); i++){
		float temp = .1 * ( stof(bookRateForFormula[i].authorAverageRating) + stof(bookRateForFormula[i].numerator) / stof(bookRateForFormula[i].total_likes));
		if(temp > max_rating){
			max_rating_index = i;
			max_rating = temp;
			//std::cout << temp << std::endl;
		}
	}
	//std::cout << max_rating_index << std::endl;
	//std::cout << bookRateForFormula[max_rating_index].id << std::endl;

	return bookRateForFormula[max_rating_index].id;

}

void thread_handler(std::vector<Book> &book, std::vector<bookRate> &rate, string &file_name){
	mtx.lock();
	ifstream input_file(file_name);
	std::string field_one;
	std::string field_two;
	std::string field_three;
	// std::vector<bookRate> rate;

	bool found_record = false;
	if (input_file.is_open())
		{
			while (getline(input_file ,field_one ,',') && !found_record)
			{				
				getline(input_file ,field_two,',');
				getline(input_file ,field_three,'\n');
				
				for (int i = 0; i < book.size(); ++i)
				{
					if (field_one == book[i].bookId)
					{
						found_record = true;
						rate.push_back({field_one, field_two, field_three});
						found_record = false;
					}
				}
			}
			//input_file.close(); <----- Closed at end of program
		}
	else std::cout << "Unable to open file.\n";
	mtx.unlock();
}

std::vector<bookRate> search_in_reviewsFile(std::vector<Book> &book)
{
	// std::string field_one;
	// std::string field_two;
	// std::string field_three;
	// std::vector<bookRate> rate;
	// std::ifstream input_file;

	std::vector<bookRate> rate;
    std::vector<thread> threads;

    int threads_count = 4;
    for(int i = 0; i < threads_count; ++i){
        // ifstream myFile("reviews_" + to_string(i+1) + ".csv");
        string file_name = "reviews_" + to_string(i+1) + ".csv";
        threads.emplace_back(thread(thread_handler, ref(book), ref(rate), ref(file_name)));
   	}

    for (auto& th : threads) 
        th.join();

	// input_file.open("/home/mohsen/Documents/OS-course-project-3/T3/testwithhuge/reviews.csv");

	// bool found_record = false;
	// if (input_file.is_open())
	// 	{
	// 		while (getline(input_file ,field_one ,',') && !found_record)
	// 		{				
	// 			getline(input_file ,field_two,',');
	// 			getline(input_file ,field_three,'\n');
				
	// 			for (int i = 0; i < book.size(); ++i)
	// 			{
	// 				if (field_one == book[i].bookId)
	// 				{
	// 					found_record = true;
	// 					rate.push_back({field_one, field_two, field_three});
	// 					found_record = false;
	// 				}
	// 			}
	// 		}
	// 		//input_file.close(); <----- Closed at end of program
	// 	}
	// else std::cout << "Unable to open file.\n";

	return rate;
}

void thread_handler_(std::vector<Book> &book, std::string &search_term, string &file_name){
	mtx.lock();
    ifstream input_file(file_name);
	std::string field_one;
	std::string field_two;
	std::string field_three;
	std::string field_four;
	std::string field_five;
	std::string field_six;
	std::string field_seven;

	bool found_record = false;
	if (input_file.is_open())
		{
			while (getline(input_file ,field_one ,',') && !found_record)
			{				
				getline(input_file ,field_two,',');
				getline(input_file ,field_three,',');
				getline(input_file ,field_four,',');
				getline(input_file ,field_five,',');
				getline(input_file ,field_six,',');
				getline(input_file ,field_seven,'\n');
				
				if (field_three == search_term || field_four == search_term )
				{
					found_record = true;
					book.push_back({field_one, field_two, field_three, field_four, field_five, field_six, field_seven}); 
					found_record = false;	
				}
			}
			/*for (int i=0;i<book.size();i++) 
			{ 
				std::cout << "book[" << i << "].bookId: " <<book[i].bookId << ", " << 
							 "book[" << i << "].bookTitle: " << book[i].bookTitle << ", " << 
				             "book[" << i << "].genre1: " << book[i].genre1 << std::endl; 
		    } */
			//input_file.close(); <----- Closed at end of program
		}
		else std::cout << "Unable to open file.\n";
		mtx.unlock();
}

std::vector<Book> Search_in_booksFile(std::string &search_term)
{
	// std::string field_one;
	// std::string field_two;
	// std::string field_three;
	// std::string field_four;
	// std::string field_five;
	// std::string field_six;
	// std::string field_seven;
	// std::ifstream input_file;
	// input_file.open("/home/mohsen/Documents/OS-course-project-3/T3/testwithhuge/books.csv");

	std::vector<Book> book;
    std::vector<thread> threads;

    int threads_count = 4;
    for(int i = 0; i < threads_count; ++i){
        // ifstream myFile("books_" + to_string(i+1) + ".csv");
        string file_name = "books_" + to_string(i+1) + ".csv";
        threads.emplace_back(thread(thread_handler_, ref(book), ref(search_term), ref(file_name)));
    }

    for (auto& th : threads) 
        th.join();

	// bool found_record = false;
	// if (input_file.is_open())
	// 	{
	// 		while (getline(input_file ,field_one ,',') && !found_record)
	// 		{				
	// 			getline(input_file ,field_two,',');
	// 			getline(input_file ,field_three,',');
	// 			getline(input_file ,field_four,',');
	// 			getline(input_file ,field_five,',');
	// 			getline(input_file ,field_six,',');
	// 			getline(input_file ,field_seven,'\n');
				
	// 			if (field_three == search_term || field_four == search_term )
	// 			{
	// 				found_record = true;
	// 				book.push_back({field_one, field_two, field_three, field_four, field_five, field_six, field_seven}); 
	// 				found_record = false;	
	// 			}
	// 		}
	// 		/*for (int i=0;i<book.size();i++) 
	// 		{ 
	// 			std::cout << "book[" << i << "].bookId: " <<book[i].bookId << ", " << 
	// 						 "book[" << i << "].bookTitle: " << book[i].bookTitle << ", " << 
	// 			             "book[" << i << "].genre1: " << book[i].genre1 << std::endl; 
	// 	    } */
	// 		//input_file.close(); <----- Closed at end of program
	// 	}
	// 	else std::cout << "Unable to open file.\n";

	return book;
}

std::vector<bookRateFull> Complete_Rank(std::vector<Book> &bookData, std::vector<bookRate> &bookRateData){
	std::vector<bookRateFull> bookRateFullData;

	for(int i = 0; i < bookRateData.size(); i++){
		for(int j = 0; j < bookData.size(); j++){
			if(bookData[j].bookId == bookRateData[i].id)
				bookRateFullData.push_back({bookRateData[i].id, bookRateData[i].rating, bookRateData[i].number_of_likes, bookData[j].authorAverageRating});
		}
	}
	return bookRateFullData;
}
