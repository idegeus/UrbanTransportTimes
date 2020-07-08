import React from 'react';
import styled from 'styled-components'
import { Link } from "react-router-dom";

import * as Shared from './components/PageComponents.jsx'
import arrowl from './images/arrow-left.svg'

import { Map, TileLayer, Marker, Popup, GeoJSON } from "react-leaflet";
import "leaflet/dist/leaflet.css";
import ReactSlider from 'react-slider'
import Cookies from 'universal-cookie';
 
const cookies = new Cookies();

class PageMap extends React.Component {

  constructor(props){
    super(props);
    this.state = {
      townhall: this.props.match.params.townhall,
      deptime: 'r',
      mode: 'fastest',
      ptime: 5,

      lat: this.props.match.params.lat,
      lon: this.props.match.params.lon,
      city: this.props.match.params.city,
      city_friendly: this.props.match.params.city.replace(/_/g, ' '),
      max_values: {},
      zoom: 12,
      current_cell: undefined,
      geojson_data: undefined,
    }
  }

  getCityGeojson = (city) => {
    fetch(`${process.env.PUBLIC_URL}/data/heatmaps/${city.replace(/ /g,"_")}.geojson`)
      .then(response => response.json())
      .then(data => this.setState({geojson_data: data}))
  }

  componentDidMount = (event) => {
    this.getCityGeojson(this.props.match.params.city)
    this.setState({ptime: parseFloat(cookies.get('ptime'))})
  }

  onEachFeature = (feature, layer) => {
    layer.on({
      mouseover: this.highlightFeature.bind(this),
      mouseout: this.resetHighlight.bind(this),
      click: this.clickToFeature.bind(this)
    });
  }

  clickToFeature =   (el) => this.setState({current_cell: el.target.feature.properties})
  highlightFeature = (el) => this.setState({hovered_cell: el.target.feature.properties})
  resetHighlight =   (el) => this.setState({hovered_cell: undefined})

  componentWillReceiveProps({ viewport }) {
    // When the provided viewport changes, apply it
    if (viewport !== this.props.viewport) {
      this.setState({ viewport })
    }
  }

  onClickReset = () => this.setState({ viewport: this.props.viewport })
  onViewportChanged = (viewport) => this.setState({ viewport })

  formatMode = mode => mode + (['car', 'pt'].includes(mode) ? '_' + this.state.deptime : '') + '_t'

  styleFeatures = feature => {
    const { properties } = feature;
    const props = properties

    if(this.state.current_cell && props.cell_id == this.state.current_cell.cell_id)
      return {
        weight: 0.5,
        opacity: 0,
        color: '#000',
        dash: false,
        fillOpacity: 0.8
      };

    switch(this.state.mode){

      case 'fastest': 

        let fastest_mode = ''
        fastest_mode = props['bike_t'] < (props[this.formatMode('car')] + this.state.ptime) ? 'bike' : 'car'
        fastest_mode = props[this.formatMode('pt')] < props[this.formatMode(fastest_mode)] ? 'pt' : fastest_mode
        fastest_mode = props['walk_t'] < props[this.formatMode(fastest_mode)] ? 'walk' : fastest_mode

        // if(props['cell_id'] == 457){
        //   console.log(feature) 
        //   console.log(fastest_mode)
        // }

        return {
          // fillColor: this.giveColor(electoralDistrict),
          weight: 0.5,
          opacity: 0,
          color: Shared.mode_colors[fastest_mode],
          dash: false,
          fillOpacity: 0.6
        };

      case 'none': 
        return {
          fillOpacity: 0
        };

      default: 
        let factor = props[this.formatMode(this.state.mode)] + (this.state.mode == 'car' && this.state.ptime)
        let value = (factor / 40) * 0.8
        let opacity = value > 0 ? 1-value : 0
        return {
          // fillColor: this.giveColor(electoralDistrict),
          weight: 0.5,
          opacity: 0,
          color: Shared.mode_colors[this.state.mode],
          dash: false,
          fillOpacity: opacity,
        };
    }
  };

  setMode = mode => this.state.mode == mode ? this.setState({mode: 'none'}) : this.setState({mode})
  decRound = num => Math.round(num * 10) / 10

  render() {

    let position = [this.state.lat, this.state.lon]
    return (
      <PageDivider>

        <Sidebar>
          <SidebarHeader />
          <SidebarOptions>
            <div style={{fontSize: '2rem', fontWeight: 'bold'}}>{this.state.city_friendly}</div>
            <div style={{fontSize: '1rem'}}>Routes to {this.state.townhall}</div>

            <OptionCollection title={'Time Length of Route'}>
              <ButtonSpreader>
                <Shared.Button 
                    text={'Driving'} mode='car' onClick={() => this.setMode('car')}
                    block active={this.state.mode == 'car'} style={{marginBottom: 10}} />
                <Shared.Button 
                    text={'Biking'}  mode='bike' onClick={() => this.setMode('bike')}
                    block active={this.state.mode == 'bike'} style={{marginBottom: 10}} />
                <Shared.Button 
                    text={'Transit'} mode='pt' onClick={() => this.setMode('pt')}
                    block active={this.state.mode == 'pt'} />
                <Shared.Button 
                    text={'Walking'} mode='walk' onClick={() => this.setMode('walk')}
                    block active={this.state.mode == 'walk'} />
              </ButtonSpreader>
            </OptionCollection>
            <OptionCollection title={'Other modes'}>
            <Shared.Button 
                    style={{padding: '0.25rem 0.5rem'}} fullBlock
                    text={<><Shared.Logo /><span>Fastest mode per cell</span></>} onClick={() => this.setMode('fastest')}
                    active={this.state.mode == 'fastest'} />
              
            </OptionCollection>

            <OptionCollection title={'Settings'}>
              <ButtonSpreader>
                <Shared.Button 
                    text={'Rush Hour (8:00)'} onClick={() => this.setState({deptime: 'r'})}
                    block center active={this.state.deptime == 'r'} />
                <Shared.Button 
                    text={'Mid-day   (12:00)'} onClick={() => this.setState({deptime: 'm'})}
                    block center active={this.state.deptime == 'm'} />
              </ButtonSpreader>
              <div style={{padding: '0.5rem'}}>
              <div style={{marginBottom: '0.5rem'}}>Parking Time</div>
                <StyledSlider
                    value={this.state.ptime} 
                    onChange={value => {cookies.set('ptime', value, { path: '/' }); this.setState({ptime: value})}}
                    renderTrack={Track}
                    renderThumb={Thumb}
                    min={0} max={15}
                />
              </div>
              Download files with this data from the repository <a href={'https://github.com/idegeus/UrbanTransportTimes'}>here</a>.
            </OptionCollection>
            <OptionCollection title={'Cell Properties'}>
              {!this.state.current_cell && <span>Select a cell to view its information.</span>}
              {this.state.current_cell && (<div>
                <div>Distance from center: {this.decRound(this.state.current_cell.sky_d)} km</div>
                <div>Estimated population: {Math.round(this.state.current_cell.cell_pop)} citizens</div>
                <table style={{marginTop: 20}}>
                  <tr><td/>
                    <td><ModeIcon mode={'car'}/></td>
                    <td><ModeIcon mode={'bike'}/></td>
                    <td><ModeIcon mode={'pt'}/></td>
                    <td><ModeIcon mode={'walk'}/></td>
                    <td/></tr>
                  <tr>
                    <td rowspan={2}>Rush Hour</td>
                    <td style={{padding: '0 0.8rem'}}>{this.decRound(this.state.current_cell.car_r_t)}</td>
                    <td style={{padding: '0 0.8rem'}}>{this.decRound(this.state.current_cell.bike_t)}</td>
                    <td style={{padding: '0 0.8rem'}}>{this.decRound(this.state.current_cell.pt_r_t)}</td>
                    <td style={{padding: '0 0.8rem'}}>{this.decRound(this.state.current_cell.walk_t)}</td>
                    <td>min</td>
                  </tr>
                  <tr>
                    <td>{this.decRound(this.state.current_cell.car_r_d)}</td>
                    <td>{this.decRound(this.state.current_cell.bike_d)}</td>
                    <td>{this.decRound(this.state.current_cell.pt_r_td)}</td>
                    <td>{this.decRound(this.state.current_cell.walk_d)}</td>
                    <td>km</td>
                  </tr>

                  <tr>
                    <td rowspan={2}>Midday</td>
                    <td>{this.decRound(this.state.current_cell.car_m_t)}</td>
                    <td>{this.decRound(this.state.current_cell.bike_t)}</td>
                    <td>{this.decRound(this.state.current_cell.pt_m_t)}</td>
                    <td>{this.decRound(this.state.current_cell.walk_t)}</td>
                    <td>min</td>
                  </tr>
                  <tr>
                    <td>{this.decRound(this.state.current_cell.car_m_d)}</td>
                    <td>{this.decRound(this.state.current_cell.bike_d)}</td>
                    <td>{this.decRound(this.state.current_cell.pt_m_td)}</td>
                    <td>{this.decRound(this.state.current_cell.walk_d)}</td>
                    <td>km</td>
                  </tr>
                </table>
              </div>)}
            </OptionCollection>

          </SidebarOptions>
        </Sidebar>

        <LeafletMapContainer>
        <Map 
          onViewportChanged={this.onViewportChanged}
          // viewport={this.state.viewport}
          center={position} 
          zoom={this.state.zoom} 
          style={{ height: "100vh" }}>
          <TileLayer
            attribution='&amp;copy <a href="http://osm.org/copyright">OpenStreetMap</a> contributors'
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />
          {this.state.geojson_data && 
            <GeoJSON 
            key='my-geojson' 
            data={this.state.geojson_data}
            style={this.styleFeatures}
            onEachFeature={this.onEachFeature.bind(this)} /> }
        </Map>
        </LeafletMapContainer>
        
      </PageDivider>
    );
  }
}

const PageDivider = styled.div`
  width: 100vw;
  height: 100vh;
  display: flex;
`

// ==== Sidebar-related stuff
const Sidebar = styled.div`
  width: 330px;
  flex: 0 0 330px;
  flex-shrink: 0;
  background: #403D39;
  height: 100%;
  color: #FFFCF2;
  z-index: 10;
`

const SidebarHeader = () => (
  <Link to={process.env.PUBLIC_URL} className='white'>
    <div style={{
        width: '100%', display: 'flex', justifyContent: 'space-between',
        padding: '1rem 1.5rem', borderBottom: '2px solid #707070', alignItems: 'center'}}>
      <span style={{fontSize: '3rem', fontWeight: 'bold'}}>DUTT</span>
      <img src={arrowl} style={{height: '1.2rem', marginLeft: 30, filter: 'invert(100%)'}} />
      <span>Homepage </span>
    </div>
  </Link>
)

const SidebarOptions = styled.div`
  padding: 1rem 1.5rem;
  color: #ffffff;
`

const OptionCollection = ({title, children}) => (
  <div>
    <div style={{
        borderBottom: '1px solid #fff', margin: '0.7rem 0', fontSize: '1.2rem', fontWeight: 'bold'}}>
      {title}
    </div>
    {children}
  </div>
)

const ButtonSpreader = styled.div`
  display: flex;
  width: 100%; 
  flex-wrap: wrap;
  justify-content: space-between;
`

const StyledSlider = styled(ReactSlider)`
    width: 100%;
    height: 25px;
`;

const StyledThumb = styled.div`
    height: 25px;
    line-height: 25px;
    width: 25px;
    text-align: center;
    background-color: #000;
    color: #fff;
    border-radius: 50%;
    cursor: grab;
`;


const Thumb = (props, state) => <StyledThumb {...props}>{state.valueNow}</StyledThumb>;

const StyledTrack = styled.div`
    top: 0;
    bottom: 0;
    background: ${props => props.index === 1 ? Shared.mode_colors['car'] : '#ddd'};
    border-radius: 999px;
`;

const Track = (props, state) => <StyledTrack {...props} index={state.index} />;

const ModeIcon = ({mode}) => (
  <div style={{width: '1.5rem', height: '1.5rem', display: 'block', padding: '0.25rem', margin: '0 auto', background: Shared.mode_colors[mode], borderRadius: 100}}>
    <img src={Shared.mode_icons[mode]} style={{height: '1rem', display: 'block', filter: 'invert(100%)'}} />
  </div>
)

// ==== Leaflet-related
const LeafletMapContainer = styled.div`
  background: limegreen;
  width: 100%;
  height: 100%;
  flex: 1;
`


export default PageMap;
