import React from 'react';
import styled from 'styled-components'
import {Trail} from 'react-spring/renderprops'
import Center from '@bedrock-layout/center'
import InlineCluster from '@bedrock-layout/inline-cluster'
import SwitchCircle from './components/SwitchCircle'

import DetourGraph from './components/DetourGraph.jsx'
import MeanSpeed from './components/MeanSpeed.jsx'
import CitiesInProject from './components/CitiesInProject.jsx'
import * as Shared from './components/PageComponents.jsx'


import walk from './images/walk.svg';

const API_SWITCHCITIES = (time, secs) => '/api/switchcities/' + time + '/' + secs

class PageMain extends React.Component {

  constructor(props){
    super(props);
    this.state = {
      departure_time: 'm',
      sort_direction: 'pt_dsc',
      bike_p_time: 0,
      car_p_time: 300,
    };
  }

  componentDidMount = (event) => {
    // this.requestSwitchTimes()
  }

  handleChange = (event) => {
    this.setState({[event.target.name]: event.target.value}, () => {
        this.requestSwitchTimes()
    }) 

    if (event.target.name == 'sort_direction'){
      
    }

  }

  requestSwitchTimes = () => {
    let request_url = API_SWITCHCITIES(
      this.state.departure_time, 
      Math.max(this.state.car_p_time - this.state.bike_p_time, 0))
    fetch(request_url)
      .then(response => response.json())
      .then(data => this.setState({ switch_cities: this.sortSwitchCities(data) }));
  }

  sortSwitchCities = switch_cities => {
    switch(this.state.sort_direction) {
      case 'city_asc': 
        return switch_cities.sort(
          (a, b) => (b.city < a.city ? 1 : (b.city > a.city ? -1 : 0)))

      case 'pt_asc': 
        return switch_cities.sort(
          (a, b) => (b.point < a.point ? 1 : (b.point > a.point ? -1 : 0)))

      default: 
      case 'pt_dsc': 
        return switch_cities.sort(
          (a, b) => (b.point < a.point ? -1 : (b.point > a.point ? 1 : 0)))
  }}

  handleSortChange = (event) => {
    this.handleChange(event)
    this.setState({switch_cities: this.sortSwitchCities(this.state.switch_cities)})
  }

  render() {
    return (
      <div className="App">
        <HeaderSpacer> <Header>DUTT</Header> <HeaderNiceMaker/> </HeaderSpacer>

        <Center style={{padding: '1rem'}}>
          <Shared.Heading1>Database of Urban Transport Times <Shared.Logo style={{display: 'inline-flex', marginLeft: '1rem'}}/> </Shared.Heading1>
          <Shared.BodyText>
            This project contains the average traffic flow in the worlds cities 
            based on actual traffic information from Bing Maps. Traffic information 
            is based on routes from all points of the city to the center (most of 
            the time city hall). Read the full paper here and find the repository 
            with all data <a href="https://github.com/idegeus/UrbanTransportTimes">here</a>.
          </Shared.BodyText>

          <CitiesInProject />

          <MeanSpeed />

          <DetourGraph/>

        </Center>
      </div>
    );
  }
}


const HeaderSpacer = styled.div`
  width: 100%;
  position: relative;
  height: 4rem;
`

const HeaderNiceMaker = styled.div`
  height: 500px;
  position: fixed;
  bottom: 99.9%;
  width: 100%;
  left: 0;
  right: 0;
  background: #403D39;
  z-index: -1;
`

const Header = styled.div`
  width: 100%;
  background: #403D39;
  padding: 1.5rem;
  height: 4rem;
  position: fixed;
  top: 0;
  left: 0;
  right:0;
  color: #FFFCF2;
`

export default PageMain;
