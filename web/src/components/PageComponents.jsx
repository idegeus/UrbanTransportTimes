import React from 'react';
import styled from 'styled-components'

import car from '../images/car.svg';
import bike from '../images/bike.svg';
import pt from '../images/pt.svg';
import walk from '../images/walk.svg';

export const Page = styled.div`
  min-height: 100vh;
  padding-bottom: 2rem;
  border-bottom: #403D39 2px solid;
  margin-bottom: 2rem;
`

export const BodyText = styled.div`
  width: 70%;
  margin-bottom: 2rem;
`

export const Heading1 = styled.h1`
  padding: 1rem;
  background: #403D39;
  text-align: left;
  display: inline-flex;
  align-items: center;
  font-weight: bold;
  font-size: 2.3rem;
  color: #FFFFFF;
`
export const Heading2 = styled.h2`
  font-size: 2rem;
  margin-bottom: 0.5rem;
  color: #403D39;
  text-align: left;
  font-weight: bold;
`

export const mode_icons = {car, bike, pt, walk}
export const mode_colors = {
  car: '#EC6A37', 
  bike: '#1481BA', 
  pt: '#3E8914', 
  walk: '#C33C54'
}

export const Logo = ({style, props}) => (
  <div {...props} style={{...{
    width: '3.5rem', height: '3.1rem', display: 'flex', flexWrap: 'wrap', 
    justifyContent: 'spaceAround', padding: 5}, ...style}}>
      {['car', 'bike', 'pt', 'walk'].map(mode => 
        <div style={{width: '1rem', height: '1rem', margin: '0.1rem', background: mode_colors[mode], borderRadius: 100}}>
          <img src={mode_icons[mode]} style={{height: '0.6rem', margin: '0.2rem', display: 'block', filter: 'invert(100%)'}} />
        </div>
      )}
  </div>
)

export const Button = ({mode, text, block, fullBlock, onClick, center, style, active, ...props}) => (
  <div 
    {...props}
    onClick={onClick}
    style={{...{padding: '0.5rem 0.8rem', 
            display: 'inline-flex',
            width: fullBlock ? '100%' : block ? '8.5rem' : 'auto',
            alignItems: 'center',
            justifyContent: 'flex-start',
            border: '2px solid #4B4843',
            textAlign: center ? 'center' : 'left',
            background: active ? '#CCC5B9' : '#4B4843',
            color: active ? '#403D39': '#fff',
            cursor: active ? 'default' : 'pointer'}, ...style}}>
      {(mode && 
        <div style={{background: mode_colors[mode], 
                     borderRadius: '100px',
                     display: 'inline-flex',
                     justifyContent: 'center',
                     alignItems: 'center',
                     width: '2rem',
                     height: '2rem',
                     flexShrink: 0,
                     marginRight: '0.8rem'}}>
          <img src={mode_icons[mode]} 
            style={{height: '1.2rem',
                    display: 'block',
                    filter: 'invert(100%)'}} /> 
        </div>)}
      {text}
  </div>
)