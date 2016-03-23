import java.util.Scanner;

public class GetRecommendations{
	
	public static void main(String args[]){
		//System.out.println("before call");
		String[] recList = null;
		try {
			Scanner sc = new Scanner(System.in);
			System.out.println("Select one of the following: \n 1. User Based Recommendation \n 1. Item Based Recommendation \n");
			int input = sc.nextInt();
			if( input !=1 && input !=2){
				System.out.println("Invalid Input selected. Please try again.");
				
			}
			else{
				if(input == 1){
				System.out.println("Enter your User ID \n");
				
				recList = HiveManager.getUserUserRecommendations(sc.next());
				}
				else{
					System.out.println("Enter the item id \n");
					recList = HiveManager.getItemItemRecommendations(sc.next());
				}
			}
			System.out.println("\n Recommended books for you are : \n");
			if(recList.length == 0){
				System.out.println("No Recommendations for this user/item.");
			}else{
			for (String string : recList) {
				if(string != null && !"".equals(string)){
					System.out.println(HiveManager.getbookTitle(string));
				}
			}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("after call");
	}
	
}