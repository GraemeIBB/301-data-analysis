## Setup instructions 
### Pt.1 mac/linux
- execute bash script `download_data.sh`. 
- create a virtual enviroment with `python3 -m venv venv`.
- source the enviroment with `source venv/bin/activate`.

### Pt.1 windows
- Open Git Bash and execute the bash script `download_data.sh`.
- Create a virtual environment with `python -m venv venv`.
- Source the environment with `source venv/Scripts/activate`.

### Pt.2 
- Copy `.env` file from `.env.example`. Make sure fields are filled with appropriate credentials.
- In the terminal, run `pip install -r requirements.txt`.
- run the python file `main.py`

#### Outputs
Ouput folders are nested such that the parent folder dictates which portion of the analysis those outputs are for. For instance, the graphs and csv files under `analysis\eda\outputs` are all pieces that were created during the EDA step of our analysis. 

#### Tableau Dashboard
An interactive dashboard visualising spend per arrival by province, origin country, and season is available on Tableau Public:
[View Dashboard](https://public.tableau.com/shared/YF24NH29R?:display_count=n&:origin=viz_share_link)

#### Link to Video Presentation
You can find our team's presentation on this analysis below! Thank you for taking the time to visit this analysis, and please reach out with any questions. 
[View Presentation Here](https://drive.google.com/drive/folders/1THvb3yLjlaMj1EZPGsMvX7cordN7Ys_7?usp=sharing)
