import React from 'react';
import styled from 'styled-components'
import * as Shared from './PageComponents.jsx'
import { Link } from "react-router-dom";

const citylist_json = process.env.PUBLIC_URL + '/data/citystats.json'
const citylist_csv = process.env.PUBLIC_URL + '/data/citystats.csv'
const fullnames_conts = {
  "EUR": 'Europe',
  "NAM": 'North America',
  'SAM': 'South America',
  "OCE": 'Oceania',
  'ASI': 'Asia',
}


class MeanSpeed extends React.Component {

    constructor(props){
      super(props);
      this.state = {
          citylist: false,
      };
  } 

  componentDidMount = (event) => {
    this.loadCities()
  }

  loadCities = () => {
     // Fetch
    fetch(citylist_json)

      // Transform to JSON
      .then(response => response.json()) 

      // Transform to an object of continents with city arrays
      .then(data => {
        let continents = {}
        data.forEach(row => 
          continents[fullnames_conts[row.continent]] = [row].concat(continents[fullnames_conts[row.continent]]) )
        return continents
      })

      // Sort city arrays per continent (key)
      .then(continents => {
        Object.keys(continents).forEach(key => 
        continents[key] = continents[key].sort((a, b) => (b.city < a.city ? 1 : (b.city > a.city ? -1 : 0))))
        return continents
      })

      // Set State.
      .then(d => {console.log('fetched', d); return d})
      .then(data => this.setState({citylist: data}))
  }

  render(){
    return (
      <Shared.Page>
        <Shared.Heading2>What cities are in this database?</Shared.Heading2>
        <Shared.BodyText size='0.9rem'>
          The analysed cities are composed of global population databases, 
          bing maps routes and OpenRouteService bicycle-routes. Click on 
          any city to visit its map or download the csv-file with data <a href={citylist_csv}>here</a>.
        </Shared.BodyText>

        <div style={{width: '100%', height: 620, display: 'flex', flexDirection: 'column', flexWrap: 'wrap'}}>
          {this.state.citylist && Object.keys(this.state.citylist).map(key => 
            <ContinentBlock key={key} continent={key} cities={this.state.citylist[key]} />)}
        </div>

      </Shared.Page>
    )
  }

}

const ContinentBlock = ({continent, cities}) => {

  return (
    <div style={{width: 300, margin: '0 2rem 2rem 0'}}>
      <div style={{borderBottom: '1px solid #EC6A37', fontSize: '1.5rem', fontWeight: 'medium', marginBottom: '0.5rem'}}>
        {continent}
      </div>
      <div style={{display: 'flex', flexDirection: 'column', maxHeight: 330, flexWrap: 'wrap'}}>
        {cities.map(city => city && 
          <Link key={city.city} to={process.env.PUBLIC_URL + `/map/${city.city.replace(/ /g,"_")}/${city.ctr_name}/${city.ctr_lat}/${city.ctr_lon}`}>
            <div style={{padding: '0.3rem 0'}} key={city.city}>{city.city}</div>
          </Link>)}
      </div>
    </div>
  )
}

export default MeanSpeed